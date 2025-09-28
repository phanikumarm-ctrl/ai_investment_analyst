# streamlit_app.py
import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
from data_ingestion import run_ingestion
from agent_core import run_analysis
from config import DATABASE_NAME

# --- Page Configuration ---
st.set_page_config(
    page_title="AI Investment Analyst",
    page_icon="🧠",
    layout="wide"
)

# --- Page Title ---
st.title("Context-Aware AI Investment Analyst 📈")
st.markdown("This application uses a multi-agent AI system to generate a daily investment briefing for the Singapore market.")

# --- Database Function ---
def get_latest_briefing():
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        df = pd.read_sql_query("SELECT briefing_date, content FROM briefings ORDER BY briefing_date DESC LIMIT 1", conn)
        conn.close()
        if not df.empty:
            return df.iloc[0]['briefing_date'], df.iloc[0]['content']
    except Exception as e:
        # This handles the case where the table doesn't exist yet
        st.error(f"Could not read briefings. Have you run the database setup? Error: {e}")
        return None, None
    return None, None

# --- Main Application ---
# Create two columns for controls and display
col1, col2 = st.columns([1, 3])

with col1:
    st.header("Controls")
    
    # Ingestion Button
    if st.button("🔄 Ingest Latest Data"):
        with st.spinner("Fetching data from APIs and storing in database..."):
            run_ingestion()
            st.success("Data ingestion complete!")

    # Analysis Button
    if st.button("🚀 Run Daily Analysis"):
        with st.spinner("🤖 Agents are analyzing the market... this may take a few minutes."):
            run_analysis()
            st.success("Daily analysis complete! Briefing is ready.")
            # Rerun to update the display
            st.experimental_rerun()

with col2:
    st.header("Latest Investment Briefing")
    
    # Display the latest briefing
    date, content = get_latest_briefing()
    
    if date:
        st.subheader(f"For Date: {date}")
        st.markdown(content)
    else:
        st.info("No briefing available. Run the daily analysis to generate one.")

# --- Add a separator ---
st.markdown("---")

# --- Data Preview Section ---
st.header("Data Previews")
preview_tabs = st.tabs(["Stock Data", "Economic Data", "News Data"])

try:
    conn = sqlite3.connect(DATABASE_NAME)
    with preview_tabs[0]:
        st.dataframe(pd.read_sql_query("SELECT * FROM sgx_stocks_daily ORDER BY record_date DESC LIMIT 10", conn))
    with preview_tabs[1]:
        st.dataframe(pd.read_sql_query("SELECT * FROM singstat_data ORDER BY record_date DESC LIMIT 10", conn))
    with preview_tabs[2]:
        st.dataframe(pd.read_sql_query("SELECT * FROM unstructured_news ORDER BY published_date DESC LIMIT 10", conn))
    conn.close()
except Exception:
    st.warning("Could not load data previews. Please run the data ingestion first.")
