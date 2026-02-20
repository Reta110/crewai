#!/bin/bash
# Ejecutar ejercicios de CrewAI
# Uso: ./run.sh <numero_ejercicio>
# Ejemplo: ./run.sh 02

source venv/bin/activate

if [ -z "$1" ]; then
    echo "🤖 Ejercicios disponibles:"
    echo "========================="
    for f in exercises/*.py; do
        # Extraer número y descripción del docstring
        num=$(basename "$f" .py | cut -d'_' -f1)
        name=$(basename "$f" .py | cut -d'_' -f2-)
        echo "  $num - $name"
    done
    echo ""
    echo "Uso: ./run.sh <numero>"
    echo "Ejemplo: ./run.sh 02"
    exit 0
fi

# Buscar el archivo que coincida con el número
FILE=$(ls exercises/${1}_*.py 2>/dev/null)

if [ -z "$FILE" ]; then
    echo "❌ Ejercicio $1 no encontrado."
    echo "Ejecuta ./run.sh sin argumentos para ver los disponibles."
    exit 1
fi

echo "▶️  Ejecutando: $FILE"
echo "========================="
arch -arm64 python "$FILE"
