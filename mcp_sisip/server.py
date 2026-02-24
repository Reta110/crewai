from fastmcp import FastMCP

from mcp_sisip.tools.crud import (
    crear_cliente,
    listar_clientes,
    crear_proveedor,
    listar_proveedores
)
from mcp_sisip.tools.analytics import (
    analisis_facturas_por_cobrar,
    analisis_facturas_por_pagar,
    consultar_db
)

mcp = FastMCP(
    name="SISIP MCP",
    instructions="""Servidor MCP para interactuar con la plataforma SISIP (Spinoff HGT).

Este servidor dispone de herramientas CRUD (vía HTTP API) y lectura analítica avanzada (vía DB).
- Para leer información, utiliza buscar_proveedores, listar_clientes o consultar_db (MySQL SELECT puro).
- Para crear información, utiliza crear_cliente, crear_proveedor, etc. No crees DB inserts directamente, SIEMPRE usa las funciones que invocan la API.
"""
)

# Registrando Herramientas CRUD
mcp.add_tool(crear_cliente)
mcp.add_tool(listar_clientes)
mcp.add_tool(crear_proveedor)
mcp.add_tool(listar_proveedores)

# Registrando Herramientas Analíticas
mcp.add_tool(analisis_facturas_por_cobrar)
mcp.add_tool(analisis_facturas_por_pagar)
mcp.add_tool(consultar_db)

if __name__ == "__main__":
    mcp.run()
