"""
Ejercicio 03 - Chat con Base de Datos vía MCP (Enhanced con Catálogo Semántico)

LECCIÓN CLAVE:
Con modelos pequeños (llama3:8b), la inteligencia debe estar en las HERRAMIENTAS,
no en el agente. El LLM no puede razonar SQL confiablemente, pero SÍ puede
presentar resultados que la herramienta ya procesó.

Arquitectura:
- UNA herramienta inteligente (ConsultarDB) que hace TODO el trabajo pesado:
  1. Usa el catálogo semántico para entender la pregunta
  2. Genera el SQL automáticamente (sin pedirle al LLM)
  3. Ejecuta la query
  4. Devuelve datos formateados
- El LLM solo tiene que presentar los resultados al usuario

Workflow:
1. python -m mcp_database.generate_catalog  (genera catalog.yaml)
2. Edita catalog.yaml: agrega aliases, cálculos, ejemplos
3. python exercises/03_mcp_database_chat.py  (chat inteligente)
"""

from crewai import Agent, Task, Crew
from crewai.tools import tool
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

os.environ["OPENAI_API_KEY"] = "NA"
os.environ["OPENAI_API_BASE"] = "http://localhost:11434/v1"
os.environ["OPENAI_MODEL_NAME"] = "llama3"
os.environ["CREWAI_TELEMETRY_OPT_OUT"] = "true"

from mcp_database.server import (
    understand_context as _understand_context,
    get_relationships as _get_relationships,
    list_tables as _list_tables,
    search_tables as _search_tables,
    search_columns as _search_columns,
    describe_table as _describe_table,
    sample_data as _sample_data,
    query as _query,
)


# ──────────────────────────────────────────────
# SMART QUERY ENGINE
# La inteligencia está aquí, no en el LLM.
# ──────────────────────────────────────────────

def _extract_numbers(text: str) -> list[int]:
    """Extrae todos los números de un texto."""
    return [int(n) for n in re.findall(r'\b\d+\b', text)]


# ──────────────────────────────────────────────
# Mapeos: columna técnica -> etiqueta humana
# ──────────────────────────────────────────────

OC_LABELS = {
    "id_oc":        "ID",
    "number":       "Número",
    "con_oc":       "Contrato",
    "fch_oc":       "Fecha",
    "pro_oc":       "Proveedor (ID)",
    "de_oc":        "Departamento",
    "ate_oc":       "Atención a",
    "mai_oc":       "Email",
    "ma2_oc":       "Email secundario",
    "obs_oc":       "Observaciones",
    "deg_oc":       "Desglose",
    "fde_oc":       "Fecha de entrega",
    "die_oc":       "Dirección de entrega",
    "est_oc":       "Estado",
    "usu_oc":       "Creado por",
    "ing_oc":       "Ingreso",
    "ref_oc":       "Referencia",
    "aut_oc":       "Autorizador",
    "au2_oc":       "Autorizador 2",
    "tip_oc":       "Tipo de OC",
    "fao_oc":       "Forma de adjudicación",
    "mon_oc":       "Moneda",
    "fpago_oc":     "Forma de pago",
    "glo_oc":       "Glosa",
    "total_oc":     "Total",
    "company_id":   "Empresa",
    "convert_oc":   "Factor conversión",
    "prp_oc":       "Presupuesto",
    "fecha_imp_oc": "Fecha impresión",
    "created_at":   "Creado el",
    "updated_at":   "Actualizado el",
}

OC_ESTADOS = {
    -1: "Anulada",
    0:  "Aplicada",
    1:  "Proveedor",
    2:  "Compras",
    3:  "Autorizar",
    4:  "Ingresando",
    5:  "Rechazada",
    9:  "VB OFC",
}

OC_MONEDAS = {
    1: "CLP (Peso Chileno)",
    2: "USD (Dólar)",
    3: "UF",
}

LINOC_LABELS = {
    "id_linoc":      "ID línea",
    "ite_linoc":     "Ítem",
    "uni_linoc":     "Unidad",
    "can_linoc":     "Cantidad",
    "pre_linoc":     "Precio unitario",
    "des_linoc":     "Descuento (%)",
    "neto_linea":    "Neto",
    "subtotal":      "Subtotal",
    "mat_linoc":     "Material",
    "par_linoc":     "Partida",
    "cpp_linoc":     "Centro de costo",
    "afe_linoc":     "Exento de IVA",
    "fin_linoc":     "Fecha inicio",
    "fte_linoc":     "Fecha término",
    "ajuste_linoc":  "Ajuste moneda",
}

IVA_RATE = 0.19

OC_SKIP = {
    "deleted_at", "deleted_by_user_id", "control_subcontrato",
    "numero_iconstruye", "monto_iconstruye", "observacion_iconstruye",
    "numero_contabilidad", "monto_contabilidad", "observacion_contabilidad",
    "numero_rendicion", "monto_rendicion", "observacion_rendicion",
    "numero_remuneraciones", "monto_remuneraciones", "observacion_remuneraciones",
    "fecha_imp_oc", "deg_oc", "fao_oc",
}

LINOC_SKIP = {
    "deleted_at", "moc_linoc", "lso_linoc", "car_linoc",
    "id_estacion", "oc_linoc",
}


def _fmt_money(value) -> str:
    """Formatea un valor monetario con separador de miles."""
    if value is None:
        return ""
    try:
        n = float(value)
        if n == int(n):
            return f"${int(n):,}".replace(",", ".")
        return f"${n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        return str(value)


def _format_oc_row(row: dict, indent: str = "  ") -> list[str]:
    """Formatea una fila de dat_oc con etiquetas legibles."""
    lines = []
    for k, v in row.items():
        if k in OC_SKIP:
            continue
        if v is None or str(v).strip() == "" or str(v).strip() == "0":
            if k not in ("id_oc", "est_oc"):
                continue

        label = OC_LABELS.get(k, k)
        display_value = v

        if k == "est_oc":
            display_value = OC_ESTADOS.get(v, v)
        elif k == "mon_oc":
            display_value = OC_MONEDAS.get(v, v)
        elif k == "total_oc" and v is not None:
            display_value = _fmt_money(v)

        lines.append(f"{indent}{label}: {display_value}")
    return lines


def _format_linoc_row(row: dict, indent: str = "    ") -> list[str]:
    """Formatea una fila de dat_linoc con etiquetas legibles."""
    lines = []
    for k, v in row.items():
        if k in LINOC_SKIP:
            continue
        if v is None or str(v).strip() == "" or str(v).strip() == "0":
            if k not in ("can_linoc", "pre_linoc", "neto_linea"):
                continue

        label = LINOC_LABELS.get(k, k)
        display_value = v

        if k in ("pre_linoc", "neto_linea", "subtotal") and v is not None:
            display_value = _fmt_money(v)
        elif k == "des_linoc" and v is not None:
            n = float(v)
            display_value = f"{n:g}%" if n > 0 else None
            if display_value is None:
                continue
        elif k == "can_linoc" and v is not None:
            n = float(v)
            display_value = int(n) if n == int(n) else n
        elif k == "afe_linoc":
            display_value = "Sí" if v == 1 else None
            if display_value is None:
                continue

        lines.append(f"{indent}{label}: {display_value}")
    return lines


def _query_safe(sql: str) -> list[dict]:
    """Ejecuta query y retorna lista vacía si hay error."""
    try:
        rows = _query(sql)
        if rows and "error" in rows[0]:
            return []
        return rows or []
    except Exception:
        return []


def _calc_neto_linea(line: dict, convert_oc: float) -> float:
    """
    Calcula el neto de una línea de OC usando la fórmula real del sistema:
    neto = (precio - precio * descuento/100) * cantidad * factor_moneda
    factor_moneda = ajuste * 1 + (1 - ajuste) * convert_oc
    """
    precio = float(line.get("pre_linoc") or 0)
    descuento = float(line.get("des_linoc") or 0)
    cantidad = float(line.get("can_linoc") or 0)
    ajuste = float(line.get("ajuste_linoc") or 0)

    factor_moneda = ajuste * 1 + (1 - ajuste) * convert_oc
    neto = (precio - (precio * descuento / 100)) * cantidad * factor_moneda
    return round(neto, 2)


def _smart_query_oc(numbers: list[int]) -> str:
    """Busca información de una Orden de Compra por número."""
    if not numbers:
        return "No se especificó un número de OC."

    output = []
    for num in numbers:
        rows = _query_safe(
            f"SELECT * FROM dat_oc WHERE id_oc = {num} AND deleted_at IS NULL"
        )

        if not rows:
            rows = _query_safe(
                f"SELECT * FROM dat_oc WHERE number = {num} AND deleted_at IS NULL"
            )

        if not rows:
            output.append(f"NO SE ENCONTRÓ la OC {num}.")
            output.append(f"(Se buscó por ID y por Número)")
            output.append(f"La OC {num} no existe en la base de datos.")

            count = _query_safe("SELECT COUNT(*) as total FROM dat_oc WHERE deleted_at IS NULL")
            if count:
                output.append(f"La base de datos tiene {count[0]['total']} OCs en total.")
            continue

        oc = rows[0]
        id_oc = oc["id_oc"]
        convert_oc = float(oc.get("convert_oc") or 1)
        mon_oc = int(oc.get("mon_oc") or 1)

        output.append(f"╔══════════════════════════════════════════╗")
        output.append(f"║  ORDEN DE COMPRA {num}")
        output.append(f"╚══════════════════════════════════════════╝")
        output.extend(_format_oc_row(oc))

        if mon_oc > 1:
            output.append(f"  Factor conversión: {convert_oc}")

        lines = _query_safe(
            f"SELECT id_linoc, ite_linoc, uni_linoc, can_linoc, pre_linoc, "
            f"des_linoc, afe_linoc, ajuste_linoc, cpp_linoc, par_linoc, mat_linoc "
            f"FROM dat_linoc WHERE oc_linoc = {id_oc} AND deleted_at IS NULL"
        )
        if lines:
            total_afecto = 0.0
            total_exento = 0.0

            output.append(f"\n┌──────────────────────────────────────────┐")
            output.append(f"│  DETALLE ({len(lines)} ítems)")
            output.append(f"└──────────────────────────────────────────┘")

            for i, line in enumerate(lines, 1):
                neto = _calc_neto_linea(line, convert_oc)
                es_exento = int(line.get("afe_linoc") or 0) == 1

                if es_exento:
                    total_exento += neto
                else:
                    total_afecto += neto

                display_line = dict(line)
                display_line["neto_linea"] = neto

                output.append(f"\n  ▸ Línea {i}:")
                output.extend(_format_linoc_row(display_line))

            iva = round(total_afecto * IVA_RATE, 2)
            total_final = total_afecto + iva + total_exento

            output.append(f"\n{'═' * 44}")
            output.append(f"  Neto Afecto:  {_fmt_money(total_afecto)}")
            output.append(f"  IVA (19%):    {_fmt_money(iva)}")
            if total_exento > 0:
                output.append(f"  Neto Exento:  {_fmt_money(total_exento)}")
            output.append(f"{'─' * 44}")
            output.append(f"  TOTAL:        {_fmt_money(total_final)}")
        else:
            output.append("\n  (Esta OC no tiene líneas de detalle)")

    return "\n".join(output)


def _smart_query_generic(question: str, numbers: list[int]) -> str:
    """Intenta responder usando el catálogo semántico + SQL directo."""
    context = _understand_context(question)
    output = [context]

    if numbers:
        output.append(f"\nNúmeros detectados en la pregunta: {numbers}")

    return "\n".join(output)


def smart_query(question: str) -> str:
    """
    Motor inteligente de consultas. Analiza la pregunta,
    identifica el patrón, genera SQL, ejecuta y retorna datos.
    """
    q_lower = question.lower()
    numbers = _extract_numbers(question)

    is_oc_query = any(kw in q_lower for kw in [
        "oc ", "oc?", " oc", "orden de compra", "orden compra",
    ])

    if is_oc_query:
        return _smart_query_oc(numbers)

    return _smart_query_generic(question, numbers)


# ──────────────────────────────────────────────
# TOOL: Una sola herramienta que hace todo
# ──────────────────────────────────────────────

@tool("ConsultarDB")
def consultar_db(pregunta: str) -> str:
    """Your ONLY tool. Pass the user's EXACT question without modifying it.
    This tool searches the database and returns the actual data found.
    Just present these results to the user in Spanish. Do NOT add, modify,
    or invent any data beyond what this tool returns."""
    try:
        return smart_query(pregunta.strip())
    except Exception as e:
        return f"Error: {e}"


# ──────────────────────────────────────────────
# AGENTE: Simple - solo presenta resultados
# ──────────────────────────────────────────────

db_analyst = Agent(
    role="Database Assistant",
    goal="Present database query results to the user clearly in Spanish",
    backstory=(
        "You help users query a database. You have ONE tool: ConsultarDB.\n"
        "RULES:\n"
        "1. Call ConsultarDB with the user's EXACT question (do not modify it)\n"
        "2. Present the results exactly as returned by the tool\n"
        "3. If the tool says data was NOT FOUND, tell the user it was not found\n"
        "4. NEVER invent, add, or modify any data\n"
        "5. Always respond in Spanish"
    ),
    verbose=True,
    allow_delegation=False,
    llm="ollama/llama3:latest",
    tools=[consultar_db],
)


def ask_database(question: str) -> str:
    """Crea una tarea ad-hoc y ejecuta el crew para responder una pregunta."""
    task = Task(
        description=(
            f"The user asks: {question}\n\n"
            "Call ConsultarDB with this EXACT question. "
            "Then present the results in Spanish. "
            "If no data was found, say 'No se encontraron datos'. "
            "NEVER invent data."
        ),
        agent=db_analyst,
        expected_output="The database results presented clearly in Spanish.",
    )
    crew = Crew(
        agents=[db_analyst],
        tasks=[task],
        verbose=True,
    )
    result = crew.kickoff()
    return str(result)


# ──────────────────────────────────────────────
# MAIN: Chat interactivo (con modo directo)
# ──────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Chat con Base de Datos (MCP + CrewAI + Catálogo Semántico)")
    print("  Escribe preguntas sobre tu DB en lenguaje natural.")
    print("  Comandos especiales:")
    print("    tablas           -> listar todas las tablas")
    print("    explorar <key>   -> explorar tablas/columnas")
    print("    contexto <preg>  -> ver contexto semántico")
    print("    relaciones <tab> -> ver relaciones de una tabla")
    print("    sql <query>      -> ejecutar SELECT directamente")
    print("    directo <preg>   -> query inteligente SIN pasar por el LLM")
    print("    generar-catalogo -> generar el catálogo semántico")
    print("    salir            -> terminar")
    print("=" * 60)

    print("\nProbando conexión a la base de datos...")
    try:
        tables = _list_tables()
        print(f"Conexión OK. Tablas encontradas: {len(tables)}")
        if tables[:5]:
            print("  " + ", ".join(tables[:5]) + ("..." if len(tables) > 5 else ""))
    except Exception as e:
        print(f"Error de conexión: {e}")
        print("Revisa tu .env (DB_HOST, DB_PORT, DB_DATABASE, DB_USERNAME, DB_PASSWORD)")
        sys.exit(1)

    catalog_path = os.path.join(os.path.dirname(__file__), "..", "mcp_database", "catalog.yaml")
    if os.path.exists(catalog_path):
        print("Catálogo semántico: CARGADO")
    else:
        print("Catálogo semántico: NO ENCONTRADO")
        print("  Genera uno con: python -m mcp_database.generate_catalog")
        print("  O escribe: generar-catalogo")

    print()

    while True:
        try:
            pregunta = input("Tu > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nHasta luego!")
            break

        if not pregunta:
            continue

        if pregunta.lower() in ("salir", "exit", "quit"):
            print("Hasta luego!")
            break

        if pregunta.lower() == "tablas":
            tables = _list_tables()
            print("Tablas encontradas:\n" + "\n".join(f"  - {t}" for t in tables))
            continue

        if pregunta.lower().startswith("explorar "):
            kw = pregunta[9:].strip()
            result = _search_tables(kw)
            if result:
                for t in result:
                    print(f"  {t['table']} ({t['columns_count']} columnas)")
                    info = _describe_table(t['table'])
                    for col in info['columns'][:10]:
                        pk = " [PK]" if col['key'] == 'PRI' else ""
                        print(f"    {col['field']} ({col['type']}){pk}")
            else:
                print(f"Nada encontrado para '{kw}'")
            continue

        if pregunta.lower().startswith("contexto "):
            q = pregunta[9:].strip()
            print(_understand_context(q))
            continue

        if pregunta.lower().startswith("relaciones "):
            t = pregunta[11:].strip()
            print(_get_relationships(t))
            continue

        if pregunta.lower().startswith("sql "):
            sql = pregunta[4:].strip()
            rows = _query_safe(sql)
            if rows:
                header = list(rows[0].keys())
                print(" | ".join(header))
                print("-" * 60)
                for row in rows[:50]:
                    print(" | ".join(str(row.get(h, "")) for h in header))
            else:
                print("Sin resultados o error en la query.")
            continue

        if pregunta.lower().startswith("directo "):
            q = pregunta[8:].strip()
            print("\n" + smart_query(q))
            continue

        if pregunta.lower() in ("generar-catalogo", "generar catalogo"):
            print("Generando catálogo semántico...")
            try:
                from mcp_database.generate_catalog import generate_catalog
                generate_catalog()
            except Exception as e:
                print(f"Error: {e}")
            continue

        print("\nPensando...\n")
        try:
            answer = ask_database(pregunta)
            print("\n" + "=" * 60)
            print("Respuesta:")
            print("=" * 60)
            print(answer)
            print()
        except Exception as e:
            print(f"\nError: {e}\n")
