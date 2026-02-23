"""
Schema Intelligence: Descubrimiento profundo automático de esquemas de BD.

Este módulo analiza una base de datos MySQL y extrae:
- Estructura completa (tablas, columnas, tipos, PKs)
- Foreign Keys explícitas
- Relaciones inferidas por convención de nombres
- Patrones de nomenclatura (prefijos de tabla/columna)
- Agrupación lógica de tablas

Uso:
    from mcp_database.schema_intelligence import SchemaIntelligence
    si = SchemaIntelligence()
    report = si.discover_all()
"""

from collections import Counter, defaultdict
from typing import Any

from mcp_database.server import get_cursor


# ──────────────────────────────────────────────
# Diccionarios de patrones comunes en ERPs latinos
# ──────────────────────────────────────────────

KNOWN_TABLE_PREFIXES = {
    "dat": "Datos transaccionales (documentos, movimientos)",
    "mae": "Tablas maestras (catálogos, entidades principales)",
    "det": "Detalle / líneas de un documento",
    "par": "Parámetros de configuración del sistema",
    "log": "Registros de auditoría / bitácora",
    "tmp": "Tablas temporales",
    "his": "Datos históricos / archivados",
    "rel": "Tablas de relación muchos-a-muchos",
    "cfg": "Configuración",
    "cat": "Catálogos",
    "tab": "Tablas auxiliares / tabuladores",
    "con": "Contabilidad / configuración contable",
    "sys": "Tablas del sistema",
    "sec": "Seguridad (usuarios, permisos)",
}

KNOWN_COLUMN_PREFIXES = {
    "cod": "Código identificador",
    "nom": "Nombre descriptivo",
    "des": "Descripción extendida",
    "fec": "Fecha",
    "can": "Cantidad",
    "pre": "Precio",
    "val": "Valor numérico / monto",
    "est": "Estado / estatus",
    "obs": "Observaciones / notas",
    "usr": "Usuario",
    "tip": "Tipo / categoría",
    "num": "Número",
    "tot": "Total",
    "mon": "Monto / moneda",
    "dir": "Dirección",
    "tel": "Teléfono",
    "rut": "RUT / identificación fiscal",
    "nro": "Número correlativo",
    "por": "Porcentaje",
    "imp": "Impuesto / importe",
    "iva": "IVA / impuesto al valor agregado",
    "nit": "NIT / identificador tributario",
    "ruc": "RUC / registro único de contribuyentes",
    "ced": "Cédula de identidad",
    "doc": "Documento",
    "ref": "Referencia",
    "sec": "Secuencia / sección",
    "lin": "Línea (de detalle)",
    "cor": "Correlativo",
    "ubi": "Ubicación",
    "bod": "Bodega / almacén",
    "art": "Artículo / producto",
    "pro": "Proveedor / producto",
    "cli": "Cliente",
    "emp": "Empresa / empleado",
    "dep": "Departamento / depósito",
    "cta": "Cuenta (contable)",
    "ord": "Orden",
    "fac": "Factura",
    "gui": "Guía (de despacho)",
    "bol": "Boleta",
    "com": "Compra / comentario",
    "ven": "Venta",
    "ing": "Ingreso",
    "egr": "Egreso",
    "sal": "Saldo / salida",
    "ent": "Entrada",
    "mov": "Movimiento",
    "tra": "Transferencia / transacción",
    "anu": "Anulado",
    "apr": "Aprobado",
    "pen": "Pendiente",
    "act": "Activo / actividad",
    "vig": "Vigente / vigencia",
}


class SchemaIntelligence:
    """Descubre automáticamente la estructura profunda de una BD MySQL."""

    def discover_all(self) -> dict:
        """Descubrimiento completo del esquema."""
        tables = self._get_all_tables()
        columns = self._get_all_columns()
        fks = self._get_foreign_keys()

        table_detail = {}
        for t in tables:
            t_cols = [c for c in columns if c["table_name"] == t]
            table_detail[t] = {
                "columns": [
                    {
                        "name": c["column_name"],
                        "type": c["column_type"],
                        "key": c["column_key"] or "",
                        "nullable": c["is_nullable"],
                        "default": c["column_default"],
                        "extra": c["extra"] or "",
                    }
                    for c in t_cols
                ],
                "primary_keys": [
                    c["column_name"] for c in t_cols if c["column_key"] == "PRI"
                ],
            }

        naming = self._analyze_naming_patterns(tables, columns)
        inferred = self._infer_relationships(table_detail, fks)
        groups = self._detect_table_groups(tables)

        return {
            "tables": table_detail,
            "foreign_keys": fks,
            "naming_patterns": naming,
            "inferred_relationships": inferred,
            "table_groups": groups,
        }

    def _get_all_tables(self) -> list[str]:
        with get_cursor() as cur:
            cur.execute("SHOW TABLES")
            rows = cur.fetchall()
        if not rows:
            return []
        key = next(iter(rows[0].keys()))
        return [r[key] for r in rows]

    def _get_all_columns(self) -> list[dict]:
        with get_cursor() as cur:
            cur.execute(
                "SELECT table_name, column_name, column_type, "
                "column_key, is_nullable, column_default, "
                "extra, ordinal_position "
                "FROM information_schema.columns "
                "WHERE table_schema = DATABASE() "
                "ORDER BY table_name, ordinal_position"
            )
            return cur.fetchall()

    def _get_foreign_keys(self) -> list[dict]:
        with get_cursor() as cur:
            cur.execute(
                "SELECT "
                "kcu.TABLE_NAME as from_table, "
                "kcu.COLUMN_NAME as from_column, "
                "kcu.REFERENCED_TABLE_NAME as to_table, "
                "kcu.REFERENCED_COLUMN_NAME as to_column, "
                "kcu.CONSTRAINT_NAME as constraint_name "
                "FROM information_schema.KEY_COLUMN_USAGE kcu "
                "WHERE kcu.TABLE_SCHEMA = DATABASE() "
                "AND kcu.REFERENCED_TABLE_NAME IS NOT NULL "
                "ORDER BY kcu.TABLE_NAME, kcu.ORDINAL_POSITION"
            )
            return cur.fetchall()

    def _analyze_naming_patterns(self, tables, columns) -> dict:
        """Detecta patrones de nomenclatura en tablas y columnas."""
        table_prefixes = Counter()
        for t in tables:
            parts = t.split("_")
            if len(parts) > 1:
                table_prefixes[parts[0]] += 1

        col_prefixes = Counter()
        for c in columns:
            parts = c["column_name"].split("_")
            if len(parts) > 1:
                col_prefixes[parts[0]] += 1

        detected_table = {}
        for prefix, count in table_prefixes.most_common(20):
            if count >= 2:
                meaning = KNOWN_TABLE_PREFIXES.get(
                    prefix, "TODO: prefijo desconocido"
                )
                detected_table[prefix + "_"] = {"count": count, "meaning": meaning}

        detected_col = {}
        for prefix, count in col_prefixes.most_common(50):
            meaning = KNOWN_COLUMN_PREFIXES.get(prefix)
            if meaning and count >= 2:
                detected_col[prefix + "_"] = {"count": count, "meaning": meaning}

        return {
            "table_prefixes": detected_table,
            "column_prefixes": detected_col,
        }

    def _infer_relationships(self, table_detail: dict, explicit_fks: list) -> list[dict]:
        """Infiere relaciones por convención de nombres cuando no hay FKs."""
        inferred = []
        explicit_pairs = set()
        for fk in explicit_fks:
            explicit_pairs.add((fk["from_table"], fk["to_table"]))
            explicit_pairs.add((fk["to_table"], fk["from_table"]))

        table_names = list(table_detail.keys())
        table_suffixes = {}
        for t in table_names:
            parts = t.split("_")
            if len(parts) >= 2:
                table_suffixes[t] = parts[-1]

        for t_name, t_info in table_detail.items():
            for col in t_info["columns"]:
                col_name = col["name"]
                col_parts = col_name.split("_")
                if len(col_parts) < 2:
                    continue

                col_prefix = col_parts[0]

                for other_table in table_names:
                    if other_table == t_name:
                        continue

                    other_suffix = table_suffixes.get(other_table)
                    if not other_suffix:
                        continue

                    if col_prefix == other_suffix:
                        pair = (t_name, other_table)
                        if pair not in explicit_pairs:
                            explicit_pairs.add(pair)
                            explicit_pairs.add((other_table, t_name))
                            inferred.append({
                                "from_table": t_name,
                                "from_column": col_name,
                                "to_table": other_table,
                                "confidence": "high",
                                "reason": (
                                    f"Columna '{col_name}' tiene prefijo '{col_prefix}' "
                                    f"que coincide con sufijo de tabla '{other_table}'"
                                ),
                            })

        for i, t1 in enumerate(table_names):
            for t2 in table_names[i + 1:]:
                s1 = table_suffixes.get(t1, "")
                s2 = table_suffixes.get(t2, "")
                if not s1 or not s2:
                    continue

                if len(s1) >= 3 and len(s2) >= 3:
                    if s1 in s2 or s2 in s1:
                        pair = (t1, t2)
                        if pair not in explicit_pairs:
                            explicit_pairs.add(pair)
                            explicit_pairs.add((t2, t1))
                            inferred.append({
                                "from_table": t1,
                                "to_table": t2,
                                "confidence": "medium",
                                "reason": (
                                    f"Sufijos de tabla comparten patrón: "
                                    f"'{s1}' / '{s2}'"
                                ),
                            })

        return inferred

    def _detect_table_groups(self, tables: list[str]) -> dict[str, list[str]]:
        """Agrupa tablas por su prefijo."""
        groups = defaultdict(list)
        for t in tables:
            parts = t.split("_")
            if len(parts) > 1:
                groups[parts[0] + "_"].append(t)
            else:
                groups["_sin_prefijo"].append(t)
        return dict(groups)

    def generate_report(self) -> str:
        """Genera un reporte legible del esquema descubierto."""
        data = self.discover_all()
        lines = []

        lines.append("=" * 70)
        lines.append("REPORTE DE INTELIGENCIA DE ESQUEMA")
        lines.append("=" * 70)

        lines.append(f"\nTablas encontradas: {len(data['tables'])}")
        lines.append(f"Foreign Keys explícitas: {len(data['foreign_keys'])}")
        lines.append(f"Relaciones inferidas: {len(data['inferred_relationships'])}")

        lines.append("\n--- PATRONES DE NOMENCLATURA ---")
        if data["naming_patterns"]["table_prefixes"]:
            lines.append("\nPrefijos de tabla:")
            for prefix, info in data["naming_patterns"]["table_prefixes"].items():
                lines.append(
                    f"  {prefix:8s} ({info['count']:3d} tablas) -> {info['meaning']}"
                )
        if data["naming_patterns"]["column_prefixes"]:
            lines.append("\nPrefijos de columna:")
            for prefix, info in data["naming_patterns"]["column_prefixes"].items():
                lines.append(
                    f"  {prefix:8s} ({info['count']:3d} columnas) -> {info['meaning']}"
                )

        lines.append("\n--- GRUPOS DE TABLAS ---")
        for group, tables in sorted(data["table_groups"].items()):
            lines.append(f"\n  [{group}] ({len(tables)} tablas):")
            for t in tables:
                pk = data["tables"][t]["primary_keys"]
                n_cols = len(data["tables"][t]["columns"])
                pk_str = ", ".join(pk) if pk else "sin PK"
                lines.append(f"    {t:40s} ({n_cols} cols, PK: {pk_str})")

        if data["foreign_keys"]:
            lines.append("\n--- FOREIGN KEYS EXPLÍCITAS ---")
            for fk in data["foreign_keys"]:
                lines.append(
                    f"  {fk['from_table']}.{fk['from_column']} -> "
                    f"{fk['to_table']}.{fk['to_column']}"
                )

        if data["inferred_relationships"]:
            lines.append("\n--- RELACIONES INFERIDAS (por convención de nombres) ---")
            for rel in data["inferred_relationships"]:
                col_info = f".{rel['from_column']}" if "from_column" in rel else ""
                lines.append(
                    f"  [{rel['confidence']:6s}] "
                    f"{rel['from_table']}{col_info} <-> {rel['to_table']}"
                )
                lines.append(f"           Razón: {rel['reason']}")

        return "\n".join(lines)
