# MCP Database – API dinámica sobre MySQL

Servidor **MCP** (Model Context Protocol) que expone una **API dinámica** sobre una base de datos MySQL: el cliente puede descubrir tablas, ver esquemas y ejecutar consultas de lectura sin tener el esquema fijo en código.

## Instalación

```bash
pip install -r requirements-mcp.txt
# o: pip install fastmcp pymysql python-dotenv
```

## Configuración

Copia las variables de entorno (por ejemplo desde `.env.example`):

```env
DB_HOST=127.0.0.1
DB_PORT=3307
DB_DATABASE=hgiplus
DB_USERNAME=root
DB_PASSWORD=root
```

Crea un `.env` en la raíz del proyecto con estos valores (o exporta las variables en el shell).

## Herramientas (tools) expuestas

| Tool | Descripción |
|------|-------------|
| `list_tables` | Lista todas las tablas de la base de datos. |
| `describe_table(table_name)` | Devuelve columnas e índices de la tabla indicada. |
| `query(sql)` | Ejecuta una consulta **SELECT** (solo lectura). INSERT/UPDATE/DELETE/DDL están bloqueados. |

## Ejecución

Desde la raíz del proyecto:

```bash
# STDIO (por defecto; para clientes MCP locales, p. ej. Cursor/Claude)
./run_mcp.sh

# HTTP (para acceso remoto)
./run_mcp.sh http
```

O directamente:

```bash
# STDIO
fastmcp run mcp_database/server.py:mcp

# HTTP en http://127.0.0.1:8000
fastmcp run mcp_database/server.py:mcp --transport http --host 127.0.0.1 --port 8000
```

## Uso en Cursor

En **Cursor** (o otro cliente MCP), añade el servidor en la configuración MCP. Ejemplo para STDIO:

```json
{
  "mcpServers": {
    "database": {
      "command": "fastmcp",
      "args": ["run", "/ruta/al/proyecto/crewai/mcp_database/server.py:mcp"]
    }
  }
}
```

Ajusta la ruta a tu `mcp_database/server.py`. Así el asistente podrá usar `list_tables`, `describe_table` y `query` sobre tu MySQL.
