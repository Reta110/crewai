from crewai import Agent, Task, Crew
import os

# Set environment variables to avoid any remote calls
os.environ["OPENAI_API_KEY"] = "NA"
os.environ["OPENAI_API_BASE"] = "http://localhost:11434/v1"
os.environ["OPENAI_MODEL_NAME"] = "llama3"
os.environ["CREWAI_TELEMETRY_OPT_OUT"] = "true"

# Define an Agent
researcher = Agent(
    role='Researcher',
    goal='Discover interesting facts about AI',
    backstory='You are a curious researcher who loves to learn new things about artificial intelligence.',
    verbose=True,
    allow_delegation=False,
    llm="ollama/llama3:latest"
)

# Define a Task
task = Task(
    description='Investigate the latest trends in agentic AI. You MUST provide a final answer that is a list of exactly 3 bullet points summarizing the trends.',
    agent=researcher,
    expected_output='A markdown list of 3 bullet points with the latest trends in agentic AI. You MUST start your final response with "Final Answer:" followed immediately by the list.'
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
