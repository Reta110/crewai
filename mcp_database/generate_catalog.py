"""
Generador automático de Catálogo Semántico.

Este script se conecta a la BD, analiza su estructura, y genera
un archivo catalog.yaml con toda la información descubierta
automáticamente + placeholders para que el humano enriquezca.

Uso:
    python -m mcp_database.generate_catalog
    python -m mcp_database.generate_catalog --output mi_catalogo.yaml

El workflow es:
1. Ejecutas este script -> genera catalog.yaml con ~80% auto-descubierto
2. Abres catalog.yaml y llenas los "TODO" -> aliases, descripciones, cálculos
3. Tu MCP usa el catálogo para responder preguntas inteligentemente

Cada vez que la BD cambie, puedes re-generar y el script preservará
tus anotaciones manuales (merge inteligente).
"""

import argparse
import os
import sys
from datetime import datetime
from typing import Any

import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv

load_dotenv()

from mcp_database.schema_intelligence import SchemaIntelligence, KNOWN_COLUMN_PREFIXES


def generate_catalog(output_path: str = None, merge_existing: bool = True):
    """Genera el catálogo semántico analizando la BD."""
    print("Conectando a la base de datos...")
    si = SchemaIntelligence()

    print("Descubriendo esquema (esto puede tomar unos segundos)...")
    schema = si.discover_all()

    existing = {}
    if output_path and os.path.exists(output_path) and merge_existing:
        print(f"Catálogo existente encontrado: {output_path}")
        print("Se preservarán tus anotaciones manuales (merge).")
        with open(output_path, encoding="utf-8") as f:
            existing = yaml.safe_load(f) or {}

    catalog = _build_catalog(schema, existing)

    if not output_path:
        output_path = os.path.join(os.path.dirname(__file__), "catalog.yaml")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(_catalog_to_yaml(catalog))

    n_tables = len(catalog.get("tables", {}))
    n_fks = len(schema.get("foreign_keys", []))
    n_inferred = len(schema.get("inferred_relationships", []))

    print(f"\nCatálogo generado exitosamente: {output_path}")
    print(f"  Tablas: {n_tables}")
    print(f"  Foreign Keys: {n_fks}")
    print(f"  Relaciones inferidas: {n_inferred}")
    print(f"\nSiguientes pasos:")
    print(f"  1. Abre {output_path}")
    print(f"  2. Busca los campos marcados 'TODO' y llénalos")
    print(f"  3. Agrega aliases, cálculos y ejemplos de tu negocio")
    print(f"  4. El MCP usará este catálogo para responder inteligentemente")


def _build_catalog(schema: dict, existing: dict) -> dict:
    """Construye el catálogo combinando auto-descubrimiento + existente."""
    existing_tables = existing.get("tables", {})
    existing_glossary = existing.get("glossary", {})
    existing_examples = existing.get("examples", [])

    catalog = {
        "database": {
            "name": existing.get("database", {}).get("name", "TODO: nombre del sistema"),
            "description": existing.get("database", {}).get(
                "description", "TODO: descripción del sistema/aplicación"
            ),
            "generated_at": datetime.now().isoformat(),
        },
        "naming_conventions": _build_naming_section(schema["naming_patterns"]),
        "glossary": _build_glossary(schema, existing_glossary),
        "tables": {},
        "examples": existing_examples or _build_example_templates(),
    }

    all_relationships = _collect_relationships(schema)

    for table_name, table_info in schema["tables"].items():
        existing_table = existing_tables.get(table_name, {})
        catalog["tables"][table_name] = _build_table_entry(
            table_name, table_info, all_relationships, existing_table
        )

    return catalog


def _build_naming_section(patterns: dict) -> dict:
    """Construye la sección de convenciones de nomenclatura."""
    result = {"table_prefixes": {}, "column_prefixes": {}}

    for prefix, info in patterns.get("table_prefixes", {}).items():
        result["table_prefixes"][prefix] = info["meaning"]

    for prefix, info in patterns.get("column_prefixes", {}).items():
        result["column_prefixes"][prefix] = info["meaning"]

    return result


def _build_glossary(schema: dict, existing_glossary: dict) -> dict:
    """Construye el glosario, preservando entradas manuales."""
    glossary = dict(existing_glossary)
    if not glossary:
        glossary["_ejemplo_OC"] = "TODO: Ejemplo - Orden de Compra (reemplaza con tus términos)"
    return glossary


def _collect_relationships(schema: dict) -> dict[str, list[dict]]:
    """Recopila todas las relaciones (FK + inferidas) indexadas por tabla."""
    rels: dict[str, list[dict]] = {}

    for fk in schema.get("foreign_keys", []):
        from_t = fk["from_table"]
        to_t = fk["to_table"]
        if from_t not in rels:
            rels[from_t] = []
        rels[from_t].append({
            "target": to_t,
            "type": "foreign_key",
            "join": f"{from_t}.{fk['from_column']} = {to_t}.{fk['to_column']}",
            "description": f"FK: {fk['from_column']} -> {to_t}.{fk['to_column']}",
        })

    for rel in schema.get("inferred_relationships", []):
        from_t = rel["from_table"]
        to_t = rel["to_table"]
        if from_t not in rels:
            rels[from_t] = []

        entry = {
            "target": to_t,
            "type": f"inferred ({rel['confidence']})",
            "description": rel["reason"],
        }
        if "from_column" in rel:
            entry["join"] = f"{from_t}.{rel['from_column']} = {to_t}.? (verificar)"

        rels[from_t].append(entry)

    return rels


def _build_table_entry(
    table_name: str,
    table_info: dict,
    all_relationships: dict,
    existing: dict,
) -> dict:
    """Construye la entrada del catálogo para una tabla."""
    entry = {
        "description": existing.get("description", "TODO: ¿Qué almacena esta tabla?"),
        "aliases": existing.get("aliases", []),
        "columns": {},
        "relationships": [],
        "calculations": existing.get("calculations", {}),
    }

    existing_cols = existing.get("columns", {})
    for col in table_info["columns"]:
        col_name = col["name"]
        if col_name in existing_cols:
            entry["columns"][col_name] = existing_cols[col_name]
        else:
            semantic = _infer_column_meaning(col_name, col["type"], col["key"])
            entry["columns"][col_name] = semantic

    existing_rels = existing.get("relationships", [])
    if existing_rels:
        entry["relationships"] = existing_rels
    elif table_name in all_relationships:
        entry["relationships"] = all_relationships[table_name]

    return entry


def _infer_column_meaning(col_name: str, col_type: str, col_key: str) -> str:
    """Infiere el significado de una columna por su nombre y tipo."""
    parts = col_name.split("_")
    prefix = parts[0] if parts else ""

    meaning_parts = []

    if col_key == "PRI":
        meaning_parts.append("[PK]")

    if prefix in KNOWN_COLUMN_PREFIXES:
        meaning_parts.append(KNOWN_COLUMN_PREFIXES[prefix])

    type_lower = col_type.lower()
    if "date" in type_lower or "time" in type_lower:
        if "Fecha" not in " ".join(meaning_parts):
            meaning_parts.append("(fecha/hora)")
    elif "decimal" in type_lower or "float" in type_lower or "double" in type_lower:
        if not any(w in " ".join(meaning_parts) for w in ["Precio", "Valor", "Cantidad", "Monto"]):
            meaning_parts.append("(numérico)")
    elif "int" in type_lower:
        if not meaning_parts:
            meaning_parts.append("(entero)")

    if meaning_parts:
        return f"{' '.join(meaning_parts)} ({col_type})"

    return f"TODO: describir ({col_type})"


def _build_example_templates() -> list[dict]:
    """Genera plantillas de ejemplo para que el usuario llene."""
    return [
        {
            "question": "TODO: Escribe una pregunta real de tu negocio",
            "sql": "TODO: El SQL correcto para esa pregunta",
            "explanation": "TODO: Explicación de la lógica",
        },
    ]


def _catalog_to_yaml(catalog: dict) -> str:
    """Serializa el catálogo a YAML con comentarios explicativos."""
    header = """# ══════════════════════════════════════════════════════════════
# CATÁLOGO SEMÁNTICO DE BASE DE DATOS
# ══════════════════════════════════════════════════════════════
#
# Este archivo es el "cerebro" que permite a la IA entender tu BD.
# Fue auto-generado y necesita tu enriquecimiento manual.
#
# CÓMO USARLO:
#   1. Busca todos los campos marcados "TODO" y llénalos
#   2. Agrega aliases: cómo los usuarios llaman a cada tabla
#      Ejemplo: dat_oc -> aliases: ["OC", "orden de compra"]
#   3. Agrega calculations: fórmulas de negocio comunes
#      Ejemplo: total_oc: "SUM(can_linoc * pre_linoc) de dat_linoc"
#   4. Agrega examples: preguntas reales con su SQL correcto
#
# REGENERAR (preserva tus cambios):
#   python -m mcp_database.generate_catalog
#
# ══════════════════════════════════════════════════════════════

"""
    yaml_str = yaml.dump(
        catalog,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
        width=120,
    )
    return header + yaml_str


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Genera el catálogo semántico desde la base de datos"
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Ruta del archivo de salida (default: mcp_database/catalog.yaml)",
    )
    parser.add_argument(
        "--no-merge",
        action="store_true",
        help="No preservar anotaciones manuales del catálogo existente",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Solo mostrar el reporte de inteligencia de esquema (no generar YAML)",
    )
    args = parser.parse_args()

    if args.report:
        si = SchemaIntelligence()
        print(si.generate_report())
    else:
        generate_catalog(
            output_path=args.output,
            merge_existing=not args.no_merge,
        )
