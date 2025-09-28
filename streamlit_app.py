# streamlit_app.py
import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
from data_ingestion import run_ingestion
from agent_core import run_analysis
from config import DATABASE_NAME

# --- Page Configuration ---
st.set_page_config(page_title="AI Investment Analyst", page_icon="🧠", layout="wide")

# --- Helper Functions to Fetch Data ---
def get_db_connection():
    return sqlite3.connect(DATABASE_NAME)

def get_latest_briefing():
    try:
        conn = get_db_connection()
        df = pd.read_sql_query("SELECT briefing_date, content FROM briefings ORDER BY briefing_date DESC LIMIT 1", conn)
        conn.close()
        return (df.iloc[0]['briefing_date'], df.iloc[0]['content']) if not df.empty else (None, None)
    except pd.errors.DatabaseError:
        return None, "Briefing table not found. Please run the analysis."
    except Exception as e:
        return None, f"An error occurred: {e}"

# --- Main App ---
st.title("Context-Aware AI Investment Analyst 📈")
st.markdown("An AI agent-driven system to analyze market data and generate daily investment briefings for the Singapore market.")

# Create tabs for different sections of the app
main_tab, eda_tab = st.tabs(["📊 Main Dashboard", "🔍 Exploratory Data Analysis"])

# --- Main Dashboard Tab ---
with main_tab:
    col1, col2 = st.columns([1, 3])
    with col1:
        st.header("Controls")
        if st.button("🔄 Ingest Latest Data"):
            with st.spinner("Fetching data from APIs and storing in database..."):
                run_ingestion()
                st.success("Data ingestion complete!")
        
        if st.button("🚀 Run Daily Analysis"):
            with st.spinner("🤖 Agents are analyzing the market... this may take a few minutes."):
                run_analysis()
                st.success("Daily analysis complete! Briefing is ready.")
                st.experimental_rerun()

    with col2:
        st.header("Latest Investment Briefing")
        date, content = get_latest_briefing()
        if date:
            st.subheader(f"For Date: {date}")
            st.markdown(content)
        else:
            st.info("No briefing available. Run data ingestion and then daily analysis to generate one.")

# --- Exploratory Data Analysis (EDA) Tab ---
with eda_tab:
    st.header("🔍 Exploratory Data Analysis")
    st.write("Visualize the raw data that the AI agents use for their analysis.")

    try:
        conn = get_db_connection()
        
        # --- Economic Indicators Visualization ---
        st.subheader("Economic Indicators")
        indicators = pd.read_sql_query("SELECT DISTINCT indicator_name FROM singstat_data", conn)['indicator_name'].tolist()
        if indicators:
            selected_indicator = st.selectbox("Select an Indicator to Visualize", options=indicators)
            if selected_indicator:
                df_econ = pd.read_sql_query(f"SELECT record_date, value FROM singstat_data WHERE indicator_name = '{selected_indicator}'", conn, parse_dates=['record_date'])
                df_econ = df_econ.set_index('record_date')
                st.line_chart(df_econ)
                st.dataframe(df_econ.sort_index(ascending=False).head())
        else:
            st.warning("No economic data found. Please run data ingestion.")

        # --- Stock Data Visualization ---
        st.subheader("SGX Stock Prices (Close)")
        tickers = pd.read_sql_query("SELECT DISTINCT ticker FROM sgx_stocks_daily", conn)['ticker'].tolist()
        if tickers:
            selected_ticker = st.selectbox("Select a Stock Ticker to Visualize", options=tickers)
            if selected_ticker:
                df_stock = pd.read_sql_query(f"SELECT record_date, close_price FROM sgx_stocks_daily WHERE ticker = '{selected_ticker}'", conn, parse_dates=['record_date'])
                df_stock = df_stock.set_index('record_date')
                st.line_chart(df_stock)
                st.dataframe(df_stock.sort_index(ascending=False).head())
        else:
            st.warning("No stock data found. Please run data ingestion.")

        conn.close()

    except Exception as e:
        st.error(f"Failed to load data for EDA. Please ensure data has been ingested. Error: {e}")
