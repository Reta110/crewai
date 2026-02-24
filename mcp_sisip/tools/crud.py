from typing import Dict, Any, List
from mcp_sisip.api.client import APIClient

def crear_cliente(company_id: int, rut: str, razon_social: str, giro: str, direccion: str, email: str, telefono: str) -> str:
    """
    Crea un nuevo Cliente enviando los datos a la API de Spinoff.
    Requiere el company_id para vincularlo a la empresa correcta.
    Debe invocarse con RUT válido (ej. 12345678-9), razón social, etc.
    """
    client = APIClient()
    data = {
        "rut_cliente": rut,
        "rs_cliente": razon_social,
        "giro_cliente": giro,
        "dir_cliente": direccion,
        "mai_cliente": email,
        "fon_cliente": telefono,
        "id_pais": "56", # Chile por defecto según HGT
        "company_id": company_id
    }
    try:
        res = client.post("spinoff/client", data)
        return f"Cliente creado exitosamente.\nRespuesta: {res}"
    except Exception as e:
        return f"Error al crear cliente: {str(e)}"

def listar_clientes(company_id: int, search: str = "") -> str:
    """
    Lista los clientes asociados a la empresa actual enviando el company_id. 
    Si se provee 'search', busca por RUT o Razón Social.
    """
    client = APIClient()
    try:
        res = client.get("spinoff/client", params={"search": search, "company_id": company_id})
        
        if "data" in res and "data" in res["data"]:
            clientes = res["data"]["data"]
            output = [f"Se encontraron {len(clientes)} clientes:\n"]
            for c in clientes:
                output.append(f"- ID: {c.get('id_cliente')} | RUT: {c.get('rut_cliente')} | Razón Social: {c.get('rs_cliente')}")
            return "\n".join(output)
        return str(res)
    except Exception as e:
        return f"Error al listar clientes: {str(e)}"

def crear_proveedor(company_id: int, rut: str, razon_social: str, giro: str, direccion: str, email: str, telefono: str) -> str:
    """
    Crea un nuevo Proveedor enviando los datos a la API de Spinoff vinculado a la empresa (company_id).
    """
    client = APIClient()
    data = {
        "rut_proveedor": rut,
        "rs_proveedor": razon_social,
        "giro_proveedor": giro,
        "dir_proveedor": direccion,
        "mai_proveedor": email,
        "fon_proveedor": telefono,
        "company_id": company_id
    }
    try:
        res = client.post("spinoff/supplier", data)
        return f"Proveedor creado exitosamente.\nRespuesta: {res}"
    except Exception as e:
        return f"Error al crear proveedor: {str(e)}"
        
def listar_proveedores(company_id: int, search: str = "") -> str:
    """
    Lista los proveedores asociados a la empresa actual (company_id). 
    Si se provee 'search', busca por RUT o Razón Social.
    """
    client = APIClient()
    try:
        res = client.get("spinoff/supplier", params={"search": search, "company_id": company_id})
        
        if "data" in res and "data" in res["data"]:
            proveedores = res["data"]["data"]
            output = [f"Se encontraron {len(proveedores)} proveedores:\n"]
            for p in proveedores:
                output.append(f"- ID: {p.get('id_proveedor')} | RUT: {p.get('rut_proveedor')} | Razón Social: {p.get('rs_proveedor')}")
            return "\n".join(output)
        return str(res)
    except Exception as e:
        return f"Error al listar proveedores: {str(e)}"
