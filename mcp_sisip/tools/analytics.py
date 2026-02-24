from mcp_sisip.db.connection import query

def analisis_facturas_por_cobrar(company_id: int) -> str:
    """
    Obtiene un resumen estructurado del total de facturas por cobrar (Ventas)
    que no han sido pagadas, basándose en la BD y excluyendo las anuladas.
    Requiere el company_id para filtrar correctamente los datos de la empresa.
    """
    sql = f'''
    SELECT 
        COUNT(id_doc) as cantidad, 
        SUM(total_doc) as total 
    FROM dat_doc 
    WHERE 
        company_id = {company_id}
        AND tip_doc IN (SELECT id_tipdoc FROM mae_tipdoc WHERE ope_tipdoc = 1 AND sub_tipdoc = 0) 
        AND est_doc NOT IN (0, -1, 14) -- No pagadas, anuladas ni neteadas
        AND deleted_at IS NULL
    '''
    try:
        res = query(sql)
        if res and res[0] and res[0]['total']:
            return f"Existen {res[0]['cantidad']} facturas de VENTA por cobrar, sumando un total de ${float(res[0]['total']):,.2f} CLP."
        return "No hay facturas de venta pendientes de cobro."
    except Exception as e:
        return f"Error al ejecutar análisis DB: {str(e)}"

def analisis_facturas_por_pagar(company_id: int) -> str:
    """
    Obtiene un resumen estructurado del total de facturas por pagar (Compras)
    que no han sido pagadas.
    Requiere el company_id para filtrar correctamente.
    """
    sql = f'''
    SELECT 
        COUNT(id_doc) as cantidad, 
        SUM(total_doc) as total 
    FROM dat_doc 
    WHERE 
        company_id = {company_id}
        AND tip_doc IN (SELECT id_tipdoc FROM mae_tipdoc WHERE ope_tipdoc = 1 AND sub_tipdoc = 1) 
        AND est_doc NOT IN (0, -1, 14) 
        AND deleted_at IS NULL
    '''
    try:
        res = query(sql)
        if res and res[0] and res[0]['total']:
            return f"Existen {res[0]['cantidad']} facturas de COMPRA por pagar, sumando un total de ${float(res[0]['total']):,.2f} CLP."
        return "No hay facturas de compra pendientes de pago."
    except Exception as e:
        return f"Error al ejecutar análisis DB: {str(e)}"
        
def consultar_db(sql_query: str, company_id: int) -> str:
    """
    Herramienta avanzada para ejecutar SELECTs directos a la base de datos de HGT (Lectura únicamente).
    Úsala para preguntas complejas como totales de un proveedor por mes.
    IMPORTANTE: El LLM SIEMPRE debe incluir el filtro 'company_id = X' en el WHERE clause de su query.
    Ejemplo: "SELECT SUM(total_doc) FROM dat_doc WHERE company_id=5 AND pro_doc=10 AND est_doc=0"
    """
    if any(keyword in sql_query.upper() for keyword in ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER']):
        return "Error: Solo se permiten consultas SELECT (solo lectura)."
        
    # Doble check de seguridad para asegurar que la query tenga el company_id
    if f"company_id" not in sql_query.lower() and str(company_id) not in sql_query:
        return f"Error: Por seguridad, todas las consultas SQL deben incluir el filtro por company_id={company_id} explícitamente en el WHERE."
        
    try:
        res = query(sql_query)
        if not res:
            return "0 resultados."
        
        # Formatear la lista de dicts en una tabla simple
        keys = list(res[0].keys())
        output = [' | '.join(keys)]
        for row in res[:50]: # limit to 50
            output.append(' | '.join(str(row[k]) for k in keys))
        
        return "\n".join(output)
    except Exception as e:
        return f"Error SQL: {str(e)}"
