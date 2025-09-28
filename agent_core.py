# agent_core.py
import sqlite3
from datetime import datetime
from langchain_google_vertexai import ChatVertexAI
from crewai import Agent, Task, Crew, Process
from tools import db_tool, news_search_tool
from config import DATABASE_NAME, VERTEX_AI_PROJECT, VERTEX_AI_LOCATION

# Initialize the LLM
llm = ChatVertexAI(
    model_name="gemini-1.5-pro-preview-0409",
    project=VERTEX_AI_PROJECT,
    location=VERTEX_AI_LOCATION,
)

# --- AGENT DEFINITIONS ---
macro_strategist = Agent(
    role='Lead Macroeconomic Strategist',
    goal='Analyze Singaporean economic indicators from the database to forecast market trends.',
    backstory='A seasoned economist from GIC, you translate raw economic data into actionable market sentiment.',
    tools=[db_tool],
    llm=llm,
    verbose=True
)

alpha_hunter = Agent(
    role='Senior SGX Equity Analyst',
    goal='Identify promising SGX-listed stocks by correlating macroeconomic trends with company performance.',
    backstory='A sharp analyst from a top hedge fund, you find undervalued stocks and growth opportunities before the market does.',
    tools=[db_tool, news_search_tool],
    llm=llm,
    verbose=True
)

risk_sentinel = Agent(
    role='Investment Risk Manager',
    goal='Assess and highlight the potential risks associated with the identified investment opportunities.',
    backstory='With a background in financial regulation at MAS, you have a keen eye for hidden risks and market volatility.',
    tools=[news_search_tool, db_tool],
    llm=llm,
    verbose=True
)

portfolio_architect = Agent(
    role='Chief Investment Officer',
    goal='Synthesize all analyses into a coherent, actionable daily investment briefing for clients.',
    backstory='You are a decisive leader, responsible for the final investment strategy. Your word is trusted and respected.',
    llm=llm,
    verbose=True
)

# --- TASK DEFINITIONS ---
# Get today's date for queries
today = datetime.now().strftime('%Y-%m-%d')

# Task 1: Macroeconomic Analysis
macro_analysis_task = Task(
    description=f"""
    1. Query the database for the latest 'CPI_All_Items' and 'Manufacturing_Output' data.
    2. Analyze the trends over the last 6 months.
    3. Read the latest news from the 'MAS' source using the news search tool.
    4. Synthesize this information to provide a brief macroeconomic outlook for Singapore for today, {today}.
    """,
    expected_output="A concise paragraph summarizing the macroeconomic outlook for Singapore, citing data points.",
    agent=macro_strategist
)

# Task 2: Stock Opportunity Identification
stock_picking_task = Task(
    description="""
    Based on the macroeconomic outlook, use the tools to analyze the performance and recent news of SGX stocks like 'C6L.SI' (SIA) and 'D05.SI' (DBS).
    Identify one 'Top Opportunity' stock and provide a clear, data-backed rationale for your choice.
    """,
    expected_output="A section titled 'Top Opportunity' with a specific stock ticker and a 2-3 sentence justification.",
    agent=alpha_hunter,
    context=[macro_analysis_task]
)

# Task 3: Risk Assessment
risk_assessment_task = Task(
    description="""
    Review the 'Top Opportunity' stock identified.
    Use the news search tool to look for any potential negative news or market headwinds related to that company or its sector.
    Summarize the key risk associated with this investment.
    """,
    expected_output="A section titled 'Key Risk Assessment' with a concise 1-2 sentence risk summary.",
    agent=risk_sentinel,
    context=[stock_picking_task]
)

# Task 4: Final Briefing Generation
briefing_creation_task = Task(
    description=f"Compile all the analyses (Macro Outlook, Top Opportunity, Risk Assessment) into a single, well-formatted investment briefing for {today}.",
    expected_output="A final, client-ready markdown document with clear headings for each section.",
    agent=portfolio_architect,
    context=[macro_analysis_task, stock_picking_task, risk_assessment_task]
)

# --- CREW DEFINITION ---
investment_crew = Crew(
    agents=[macro_strategist, alpha_hunter, risk_sentinel, portfolio_architect],
    tasks=[macro_analysis_task, stock_picking_task, risk_assessment_task, briefing_creation_task],
    process=Process.sequential,
    verbose=2
)

def run_analysis():
    """Kicks off the crew analysis and returns the final briefing."""
    result = investment_crew.kickoff()
    
    # Save the result to the database
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO briefings (briefing_date, content) VALUES (?, ?)", (today, result))
    conn.commit()
    conn.close()
    
    return result
