import os
import sys
from dotenv import load_dotenv
from crewai import Agent, Task, Crew
from crewai.tools import tool

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
load_dotenv()

os.environ["OPENAI_API_KEY"] = "NA"
os.environ["OPENAI_API_BASE"] = "http://localhost:11434/v1"
os.environ["OPENAI_MODEL_NAME"] = "llama3"
os.environ["CREWAI_TELEMETRY_OPT_OUT"] = "true"

from mcp_sisip.tools.crud import crear_cliente, listar_clientes, crear_proveedor, listar_proveedores
from mcp_sisip.tools.analytics import analisis_facturas_por_cobrar, analisis_facturas_por_pagar, consultar_db

@tool("CrearCliente")
def tool_crear_cliente(company_id: int, rut: str, razon_social: str, giro: str, direccion: str, email: str, telefono: str) -> str:
    """Crea un nuevo Cliente en SISIP. Requiere: company_id, rut, razon_social, giro, direccion, email, telefono."""
    return crear_cliente(company_id, rut, razon_social, giro, direccion, email, telefono)

@tool("ListarClientes")
def tool_listar_clientes(company_id: int, search: str = "") -> str:
    """Busca o lista clientes en SISIP para una empresa dada."""
    return listar_clientes(company_id, search)

@tool("CrearProveedor")
def tool_crear_proveedor(company_id: int, rut: str, razon_social: str, giro: str, direccion: str, email: str, telefono: str) -> str:
    """Crea un nuevo Proveedor en SISIP. Requiere: company_id, rut, razon_social, giro, direccion, email, telefono."""
    return crear_proveedor(company_id, rut, razon_social, giro, direccion, email, telefono)

@tool("ListarProveedores")
def tool_listar_proveedores(company_id: int, search: str = "") -> str:
    """Busca o lista proveedores en SISIP para una empresa dada."""
    return listar_proveedores(company_id, search)

@tool("FacturasPorCobrar")
def tool_facturas_cobrar(company_id: int) -> str:
    """Devuelve el total y cantidad de facturas de VENTA pendientes de cobro para la empresa."""
    return analisis_facturas_por_cobrar(company_id)

@tool("FacturasPorPagar")
def tool_facturas_pagar(company_id: int) -> str:
    """Devuelve el total y cantidad de facturas de COMPRA pendientes de pago para la empresa."""
    return analisis_facturas_por_pagar(company_id)

@tool("ConsultarDB")
def tool_consultar_db(query: str, company_id: int) -> str:
    """Ejecuta una consulta SQL SELECT cruda en la base de datos de SISIP/HGT. Úsalo para reportes avanzados de la compañía especificada."""
    return consultar_db(query, company_id)

sisip_agent = Agent(
    role="Asistente Administrativo de SISIP",
    goal="Ayudar al usuario a gestionar clientes, proveedores y responder consultas analíticas financieras.",
    backstory=(
        "Eres un asistente experto en el ERP SISIP. Tienes acceso a herramientas API "
        "para crear y listar clientes/proveedores de forma segura. "
        "También tienes acceso directo a la base de datos (ReadOnly) para responder preguntas financieras "
        "o listar facturas.\n\n"
        "REGLAS:\n"
        "1. Para CREAR registros, usa SOLO las herramientas de Crear (nunca intentes un INSERT a la BD).\n"
        "2. Para REPORTES, usa ConsultarDB, FacturasPorCobrar, o FacturasPorPagar.\n"
        "3. Siempre responde en Español, de manera amable y profesional."
    ),
    verbose=True,
    allow_delegation=False,
    llm="ollama/llama3:latest",
    tools=[
        tool_crear_cliente, tool_listar_clientes,
        tool_crear_proveedor, tool_listar_proveedores,
        tool_facturas_cobrar, tool_facturas_pagar,
        tool_consultar_db
    ]
)

def ask_sisip(question: str) -> str:
    """Ejecuta el crew para responder a la pregunta."""
    task = Task(
        description=f"El usuario te solicita o pregunta: '{question}'\n\nResuelve su solicitud utilizando las herramientas adecuadas.",
        agent=sisip_agent,
        expected_output="Respuesta clara al usuario confirmando la acción realizada o los datos solicitados."
    )
    crew = Crew(
        agents=[sisip_agent],
        tasks=[task],
        verbose=True
    )
    return str(crew.kickoff())

if __name__ == "__main__":
    print("=" * 60)
    print("  Asistente Administrativo SISIP (MCP Local - API & DB)")
    print("  Escribe solicitudes como:")
    print("   - 'Lista mis clientes'")
    print("   - 'Registra un cliente con RUT 123456-7, nombre Empresa Acme, etc...'")
    print("   - '¿Cuánto dinero tenemos por cobrar?'")
    print("   - 'salir' para terminar")
    print("=" * 60)

    while True:
        try:
            q = input("\nUsuario > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n¡Hasta luego!")
            break

        if not q or q.lower() in ("salir", "exit", "quit"):
            print("¡Hasta luego!")
            break

        print("\nPensando...\n")
        try:
            answer = ask_sisip(q)
            print("\n" + "=" * 60)
            print("SISIP Asistente:")
            print("=" * 60)
            print(answer)
        except Exception as e:
            print(f"\nError: {e}\n")
