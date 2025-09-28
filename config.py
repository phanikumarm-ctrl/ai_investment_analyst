# config.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- LLM Configuration ---
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
VERTEX_AI_PROJECT = os.getenv("VERTEX_AI_PROJECT")
VERTEX_AI_LOCATION = os.getenv("VERTEX_AI_LOCATION")

# --- Database Configuration ---
DATABASE_NAME = "investment_analysis.db"
