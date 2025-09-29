# tools.py
import sqlite3
import pandas as pd
from crewai.tools import BaseTool
from config import DATABASE_NAME

class DatabaseTool(BaseTool):
    name: str = "SQL Database Query Tool"
    description: str = "Executes a SQL query against the local SQLite database. Use this to get all structured data, including economic indicators, stock prices, and past briefings."

    def _run(self, query: str) -> str:
        try:
            conn = sqlite3.connect(DATABASE_NAME)
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df.to_string()
        except Exception as e:
            return f"Error executing query: {e}"

class SemanticSearchTool(BaseTool):
    name: str = "Semantic News Search Tool"
    description: str = "Searches for unstructured news and reports based on a topic. Use this to find qualitative context about a company or economic event."

    def _run(self, topic: str) -> str:
        # NOTE: This is a simplified simulation of semantic search for local testing.
        # In production, this would query a real vector database like Vertex AI Vector Search.
        try:
            conn = sqlite3.connect(DATABASE_NAME)
            df = pd.read_sql_query("SELECT source, published_date, content FROM unstructured_news", conn)
            conn.close()
            
            # Simple keyword matching for simulation
            results = df[df['content'].str.contains(topic, case=False)]
            if results.empty:
                return f"No news found on the topic: {topic}"
            return results.to_string()
        except Exception as e:
            return f"Error searching news: {e}"

# Instantiate tools for agents
db_tool = DatabaseTool()
news_search_tool = SemanticSearchTool()
