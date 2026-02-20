"""
Ejercicio 02 - Crew de Canción Infantil
Dos agentes trabajando en secuencia:
  1. Agente Investigador: Busca temas tendencia para niños
  2. Agente Escritor: Toma esos temas y escribe una canción con rimas pegajosas

La salida de la Tarea 1 (investigación) alimenta la Tarea 2 (escritura).
"""

from crewai import Agent, Task, Crew, Process
import os

# Configuración para usar Ollama local
os.environ["OPENAI_API_KEY"] = "NA"
os.environ["OPENAI_API_BASE"] = "http://localhost:11434/v1"
os.environ["OPENAI_MODEL_NAME"] = "llama3"
os.environ["CREWAI_TELEMETRY_OPT_OUT"] = "true"

# ──────────────────────────────────────────────
# AGENTE 1: Investigador de Tendencias Infantiles
# ──────────────────────────────────────────────
researcher = Agent(
    role='Investigador de Tendencias Infantiles',
    goal='Descubrir los temas más populares y emocionantes que están de moda entre niños de 4 a 10 años',
    backstory=(
        'Eres un experto en cultura infantil y tendencias de entretenimiento para niños. '
        'Conoces los temas que más emocionan a los pequeños: desde dinosaurios espaciales '
        'hasta robots que hacen pizza. Siempre estás al día con lo que los niños aman.'
    ),
    verbose=True,
    allow_delegation=False,
    llm="ollama/llama3:latest"
)

# ──────────────────────────────────────────────
# AGENTE 2: Escritor de Canciones Infantiles
# ──────────────────────────────────────────────
songwriter = Agent(
    role='Escritor de Canciones Infantiles',
    goal='Escribir letras de canciones divertidas, pegajosas y fáciles de cantar para niños',
    backstory=(
        'Eres un compositor legendario de canciones infantiles. '
        'Has escrito éxitos que los niños cantan en todo el mundo. '
        'Tus canciones tienen rimas pegajosas, coros repetitivos y son súper divertidas. '
        'Siempre incluyes onomatopeyas y palabras que hacen reír a los niños.'
    ),
    verbose=True,
    allow_delegation=False,
    llm="ollama/llama3:latest"
)

# ──────────────────────────────────────────────
# TAREA 1: Investigar temas tendencia para niños
# ──────────────────────────────────────────────
research_task = Task(
    description=(
        'Investiga y genera una lista de 3 temas creativos y divertidos que están '
        'de moda entre niños de 4 a 10 años. Los temas deben ser combinaciones '
        'imaginativas y emocionantes, como por ejemplo: "dinosaurios espaciales", '
        '"unicornios programadores", "piratas en el fondo del mar". '
        'Para cada tema, incluye una breve descripción de por qué es popular entre los niños.'
    ),
    agent=researcher,
    expected_output=(
        'Una lista de 3 temas creativos con su descripción. '
        'Formato: nombre del tema + por qué les encanta a los niños.'
    )
)

# ──────────────────────────────────────────────
# TAREA 2: Escribir la canción basada en la investigación
# ──────────────────────────────────────────────
songwriting_task = Task(
    description=(
        'Usando los temas de tendencias infantiles proporcionados por el investigador, '
        'escribe la letra de una canción original para niños en español. '
        'La canción debe: \n'
        '- Tener al menos 2 estrofas y un coro pegajoso\n'
        '- Incluir rimas divertidas y fáciles\n'
        '- Usar al menos 2 de los temas investigados\n'
        '- Incluir onomatopeyas (¡boom!, ¡splash!, ¡zum!)\n'
        '- Ser fácil de cantar y memorizar para niños de 4 a 10 años'
    ),
    agent=songwriter,
    expected_output=(
        'La letra completa de una canción infantil en español con título, '
        'estrofas claramente separadas y un coro marcado.'
    )
)

# ──────────────────────────────────────────────
# CREW: Equipo secuencial (investigar → escribir)
# ──────────────────────────────────────────────
crew = Crew(
    agents=[researcher, songwriter],
    tasks=[research_task, songwriting_task],
    process=Process.sequential,  # La salida de tarea 1 alimenta tarea 2
    verbose=True
)

if __name__ == "__main__":
    print("🎵 Iniciando Crew de Canción Infantil...")
    print("=" * 50)
    
    result = crew.kickoff()
    
    print("\n" + "=" * 50)
    print("🎶 ¡CANCIÓN TERMINADA!")
    print("=" * 50)
    print(result)
