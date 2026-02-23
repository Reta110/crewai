"""
Servidor MCP (Model Context Protocol) con API dinámica sobre MySQL.

Herramientas de descubrimiento (explorar antes de consultar):
- understand_context: LA HERRAMIENTA PRINCIPAL - busca contexto semántico de negocio
- list_tables: lista las tablas de la base de datos
- search_tables: busca tablas cuyo nombre contenga una palabra clave
- search_columns: busca columnas en toda la DB que coincidan con una palabra clave
- describe_table: describe columnas e índices de una tabla
- sample_data: muestra filas de ejemplo de una tabla para entender los datos
- get_relationships: muestra las relaciones de una tabla con otras

Herramientas de consulta:
- query: ejecuta una consulta SQL de solo lectura (SELECT)
"""
import os
import re
from contextlib import contextmanager
from typing import Any

import pymysql
from fastmcp import FastMCP

from mcp_database.config import get_db_config


def _connection():
    """Crea una conexión MySQL con la config del entorno."""
    cfg = get_db_config()
    if not cfg["database"]:
        raise ValueError(
            "DB_DATABASE no está definido. Configura .env con DB_HOST, DB_PORT, "
            "DB_DATABASE, DB_USERNAME, DB_PASSWORD."
        )
    return pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
        charset=cfg["charset"],
    )


@contextmanager
def get_cursor(dict_cursor: bool = True):
    """Context manager para obtener un cursor sobre la DB."""
    conn = _connection()
    try:
        cur = conn.cursor(
            pymysql.cursors.DictCursor if dict_cursor else pymysql.cursors.Cursor
        )
        yield cur
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _safe_table_name(name: str) -> str:
    """Permite solo caracteres seguros para nombres de tabla (evita SQL injection)."""
    if not name or not re.match(r"^[a-zA-Z0-9_]+$", name):
        raise ValueError("Nombre de tabla no válido (solo letras, números y _)")
    return name


# SQL de solo lectura: solo permitimos SELECT (sin subconsultas que modifiquen datos)
_SELECT_ONLY = re.compile(
    r"^\s*(WITH\s+[\w\s,]+\s+AS\s*\([^)]*\)\s*)?\s*SELECT\s+",
    re.IGNORECASE | re.DOTALL,
)
_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|REPLACE|EXEC|EXECUTE)\b",
    re.IGNORECASE,
)


def _is_readonly_sql(sql: str) -> bool:
    """Comprueba que la sentencia sea solo de lectura (SELECT)."""
    stripped = sql.strip()
    if not stripped:
        return False
    if _FORBIDDEN.search(stripped):
        return False
    return bool(_SELECT_ONLY.match(stripped))


# --- Semantic Catalog (optional but powerful) ---

_catalog = None
_CATALOG_PATH = os.path.join(os.path.dirname(__file__), "catalog.yaml")


def _get_catalog():
    """Lazy-load del catálogo semántico."""
    global _catalog
    if _catalog is None and os.path.exists(_CATALOG_PATH):
        from mcp_database.semantic_catalog import SemanticCatalog
        try:
            _catalog = SemanticCatalog(_CATALOG_PATH)
        except Exception:
            _catalog = False  # sentinel: tried and failed
    return _catalog if _catalog and _catalog is not False else None


# --- FastMCP Server ---

mcp = FastMCP(
    name="Database MCP",
    instructions="""Este servidor expone una API dinámica sobre una base de datos MySQL.

PROTOCOLO OBLIGATORIO para responder preguntas:

1. CONTEXTO: SIEMPRE usa understand_context PRIMERO con la pregunta del usuario.
   Esta herramienta te dará el significado de negocio, tablas relevantes,
   relaciones, fórmulas de cálculo y ejemplos de queries similares.

2. Si understand_context da suficiente información, ve directo al paso 4.

3. Si necesitas más detalle: usa describe_table y sample_data para explorar.

4. CONSULTAR: Construye y ejecuta la consulta SQL con query.

Herramientas disponibles (en orden de prioridad):
- understand_context(question): SIEMPRE PRIMERO - te da todo el contexto de negocio
- get_relationships(table_name): ver relaciones de una tabla con otras
- search_tables(keyword): busca tablas por palabra clave
- search_columns(keyword): busca columnas en TODAS las tablas
- describe_table(table_name): muestra columnas e índices de una tabla
- sample_data(table_name, limit): muestra filas de ejemplo
- query(sql): ejecuta SELECT (solo lectura)

NUNCA adivines nombres de columnas. SIEMPRE usa understand_context primero.""",
)


@mcp.tool()
def understand_context(question: str) -> str:
    """
    HERRAMIENTA PRINCIPAL - Usa SIEMPRE PRIMERO.

    Analiza tu pregunta y devuelve contexto semántico de negocio:
    - Glosario: qué significan las abreviaturas (OC = Orden de Compra)
    - Tablas relevantes con su descripción y columnas clave
    - Relaciones entre tablas (JOINs correctos)
    - Fórmulas de cálculo (ej: total OC = SUM(can * pre))
    - Ejemplos de queries similares

    Ejemplo: understand_context("¿Cuál es el total de la OC 58136?")
    Te dirá exactamente qué tablas usar, cómo hacer el JOIN y la fórmula.
    """
    catalog = _get_catalog()
    if not catalog:
        return (
            "Catálogo semántico no disponible.\n"
            "Usa search_tables y describe_table para explorar manualmente.\n"
            "Para habilitar contexto inteligente, genera el catálogo con:\n"
            "  python -m mcp_database.generate_catalog"
        )
    return catalog.get_relevant_context(question)


@mcp.tool()
def get_relationships(table_name: str) -> str:
    """
    Muestra las relaciones de una tabla con otras tablas.
    Incluye: Foreign Keys, relaciones inferidas, y JOINs sugeridos.
    Útil para entender cómo conectar tablas en consultas.
    """
    catalog = _get_catalog()
    parts = []

    if catalog:
        info = catalog.get_table_info(table_name)
        if info:
            if info.get("description"):
                parts.append(f"Tabla: {table_name}")
                parts.append(f"Descripción: {info['description']}")
            if info.get("relationships"):
                parts.append("\nRelaciones (del catálogo):")
                for rel in info["relationships"]:
                    parts.append(f"  -> {rel['target']} ({rel.get('type', '?')})")
                    if rel.get("description"):
                        parts.append(f"     {rel['description']}")
                    if rel.get("join"):
                        parts.append(f"     JOIN: {rel['join']}")
            if info.get("calculations"):
                parts.append("\nCálculos comunes:")
                for name, formula in info["calculations"].items():
                    parts.append(f"  {name}: {formula}")

    safe_name = _safe_table_name(table_name)
    with get_cursor() as cur:
        cur.execute(
            "SELECT kcu.COLUMN_NAME as col, "
            "kcu.REFERENCED_TABLE_NAME as ref_table, "
            "kcu.REFERENCED_COLUMN_NAME as ref_col "
            "FROM information_schema.KEY_COLUMN_USAGE kcu "
            "WHERE kcu.TABLE_SCHEMA = DATABASE() "
            "AND kcu.TABLE_NAME = %s "
            "AND kcu.REFERENCED_TABLE_NAME IS NOT NULL",
            (safe_name,),
        )
        fks_from = cur.fetchall()

        cur.execute(
            "SELECT kcu.TABLE_NAME as from_table, "
            "kcu.COLUMN_NAME as from_col, "
            "kcu.REFERENCED_COLUMN_NAME as ref_col "
            "FROM information_schema.KEY_COLUMN_USAGE kcu "
            "WHERE kcu.TABLE_SCHEMA = DATABASE() "
            "AND kcu.REFERENCED_TABLE_NAME = %s",
            (safe_name,),
        )
        fks_to = cur.fetchall()

    if fks_from:
        parts.append("\nFK salientes (esta tabla referencia a):")
        for fk in fks_from:
            parts.append(
                f"  {table_name}.{fk['col']} -> "
                f"{fk['ref_table']}.{fk['ref_col']}"
            )

    if fks_to:
        parts.append("\nFK entrantes (estas tablas referencian a esta):")
        for fk in fks_to:
            parts.append(
                f"  {fk['from_table']}.{fk['from_col']} -> "
                f"{table_name}.{fk['ref_col']}"
            )

    if not parts:
        return f"No se encontró información de relaciones para '{table_name}'."

    return "\n".join(parts)


@mcp.tool()
def list_tables() -> list[str]:
    """Lista los nombres de todas las tablas de la base de datos actual."""
    with get_cursor() as cur:
        cur.execute("SHOW TABLES")
        rows = cur.fetchall()
    if not rows:
        return []
    # DictCursor con SHOW TABLES devuelve {"Tables_in_<db>": "nombre"}
    key = next(iter(rows[0].keys()), None)
    if not key:
        return []
    return [r[key] for r in rows]


@mcp.tool()
def search_tables(keyword: str) -> list[dict[str, str]]:
    """
    Busca tablas cuyo nombre contenga la palabra clave (búsqueda parcial).
    Ejemplo: search_tables("contrato") -> encuentra "mae_contrato", "det_contrato", etc.
    Devuelve nombre de tabla y cantidad de columnas.
    """
    keyword_lower = keyword.lower().strip()
    if not keyword_lower:
        return [{"error": "Debes proporcionar una palabra clave"}]
    with get_cursor() as cur:
        cur.execute("SHOW TABLES")
        rows = cur.fetchall()
    if not rows:
        return []
    key = next(iter(rows[0].keys()))
    matches = [r[key] for r in rows if keyword_lower in r[key].lower()]
    result = []
    for table in matches:
        with get_cursor() as cur:
            cur.execute("SELECT COUNT(*) as cnt FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = %s", (table,))
            col_count = cur.fetchone()["cnt"]
        result.append({"table": table, "columns_count": str(col_count)})
    return result


@mcp.tool()
def search_columns(keyword: str) -> list[dict[str, str]]:
    """
    Busca columnas en TODAS las tablas cuyo nombre contenga la palabra clave.
    Ejemplo: search_columns("nombre") -> encuentra columnas como "nom_contrato", "nombre_area", etc.
    Devuelve tabla, columna y tipo de dato. Útil para descubrir dónde está un dato.
    """
    keyword_lower = keyword.lower().strip()
    if not keyword_lower:
        return [{"error": "Debes proporcionar una palabra clave"}]
    with get_cursor() as cur:
        cur.execute(
            "SELECT table_name, column_name, column_type, column_key "
            "FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND LOWER(column_name) LIKE %s "
            "ORDER BY table_name, ordinal_position",
            (f"%{keyword_lower}%",),
        )
        rows = cur.fetchall()
    return [
        {
            "table": r["table_name"],
            "column": r["column_name"],
            "type": r["column_type"],
            "key": r["column_key"] or "",
        }
        for r in rows
    ]


@mcp.tool()
def sample_data(table_name: str, limit: int = 5) -> list[dict[str, Any]]:
    """
    Muestra filas de ejemplo de una tabla para entender el formato de los datos.
    Usa esto ANTES de construir consultas para saber cómo lucen los valores reales
    (formatos de código, convenciones de nombres, etc.).
    """
    safe_name = _safe_table_name(table_name)
    limit = min(max(1, limit), 10)
    with get_cursor() as cur:
        cur.execute("SELECT * FROM `%s` LIMIT %s" % (safe_name, limit))
        return cur.fetchall()


@mcp.tool()
def describe_table(table_name: str) -> dict[str, Any]:
    """
    Describe la estructura de una tabla: columnas (nombre, tipo, null, key, default)
    e índices. Pasa el nombre exacto de la tabla (sensible a mayúsculas según el SO).
    """
    safe_name = _safe_table_name(table_name)
    with get_cursor() as cur:
        cur.execute("SHOW FULL COLUMNS FROM `%s`" % safe_name)
        columns = cur.fetchall()
        cur.execute("SHOW INDEX FROM `%s`" % safe_name)
        indexes = cur.fetchall()
    return {
        "columns": [
            {
                "field": c.get("Field"),
                "type": c.get("Type"),
                "null": c.get("Null"),
                "key": c.get("Key"),
                "default": c.get("Default"),
                "extra": c.get("Extra"),
            }
            for c in columns
        ],
        "indexes": [
            {
                "name": i.get("Key_name"),
                "column": i.get("Column_name"),
                "unique": i.get("Non_unique") == 0,
            }
            for i in indexes
        ],
    }


@mcp.tool()
def query(sql: str) -> list[dict[str, Any]]:
    """
    Ejecuta una consulta SQL de solo lectura (SELECT). Devuelve las filas como lista de diccionarios.
    Solo se permiten sentencias SELECT; INSERT/UPDATE/DELETE y DDL están bloqueados.
    """
    if not _is_readonly_sql(sql):
        return [
            {
                "error": "Solo se permiten consultas SELECT. No se permiten INSERT, UPDATE, DELETE, DROP, etc."
            }
        ]
    with get_cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


if __name__ == "__main__":
    mcp.run()
