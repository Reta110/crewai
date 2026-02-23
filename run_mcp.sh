#!/bin/bash
# Ejecutar servidor MCP de base de datos (MySQL)
# Requiere: .env con DB_HOST, DB_PORT, DB_DATABASE, DB_USERNAME, DB_PASSWORD
# Uso: ./run_mcp.sh [stdio|http]

set -e
cd "$(dirname "$0")"

if [ ! -d "venv" ]; then
  echo "Crea un venv y activa: python -m venv venv && source venv/bin/activate"
  echo "Luego: pip install -r requirements-mcp.txt"
  exit 1
fi

source venv/bin/activate

if [ ! -f ".env" ]; then
  echo "No hay .env. Copia .env.example a .env y configura DB_*"
  exit 1
fi

TRANSPORT="${1:-stdio}"

if [ "$TRANSPORT" = "http" ]; then
  echo "Iniciando servidor MCP (HTTP) en http://127.0.0.1:8000"
  fastmcp run mcp_database/server.py:mcp --transport http --host 127.0.0.1 --port 8000
else
  echo "Iniciando servidor MCP (STDIO). Conectar desde cliente MCP."
  fastmcp run mcp_database/server.py:mcp
fi
