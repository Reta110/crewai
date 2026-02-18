from crewai import Agent, Task, Crew
from crewai.tools import tool
from langchain_community.tools import DuckDuckGoSearchRun
import os

# Set environment variables to avoid any remote calls
os.environ["OPENAI_API_KEY"] = "NA"
os.environ["OPENAI_API_BASE"] = "http://localhost:11434/v1"
os.environ["OPENAI_MODEL_NAME"] = "llama3"
os.environ["CREWAI_TELEMETRY_OPT_OUT"] = "true"

@tool("DuckDuckGoSearch")

def search_tool(search_query: str):
    """Useful to search on internet about a specific topic and returns relevant results"""
return DuckDuckGoSearchRun().run(search_query)

# Define an Agent
researcher = Agent(
    role='Researcher',
    goal='Descubrir los resultados del juego de las americas',
    backstory='Eres un fanatico deportivo que le gustan las estatdíticas.',
    verbose=True,
    allow_delegation=False,
    llm="ollama/llama3:latest",
    tools=[search_tool]
)

# Define a Task
task = Task(
    description='Descubrir resultado de ultimo partido de magallanes en el juego de las americas.',
    agent=researcher,
    expected_output='Solo el resultado del partido.'
)

# Define a Crew
crew = Crew(
    agents=[researcher],
    tasks=[task],
    verbose=True
)

# Run the Crew
result = crew.kickoff()
print("######################")
print(result)
