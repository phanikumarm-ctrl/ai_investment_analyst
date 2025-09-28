# streamlit_app.py
import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
from data_ingestion import run_ingestion
from agent_core import run_analysis
from predictive_models import generate_forecasts # Import the new function
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
# --- Main App ---
st.title("Predictive AI Investment Analyst 🔮")
st.markdown("An AI agent-driven system, enhanced with ML forecasting, to generate forward-looking investment briefings.")

main_tab, eda_tab = st.tabs(["📊 Main Dashboard", "🔍 Exploratory Data Analysis"])

# --- Main Dashboard Tab ---
with main_tab:
    col1, col2 = st.columns([1, 3])
    with col1:
        st.header("Controls")
        st.markdown("Follow these steps in order:")
        
        # Step 1: Ingestion
        if st.button("1. Ingest Latest Data"):
            with st.spinner("Fetching latest historical data..."):
                run_ingestion()
                st.success("Data ingestion complete!")
        
        # Step 2: Prediction
        if st.button("2. Generate Forecasts"):
            with st.spinner("🤖 Training models and predicting future values..."):
                generate_forecasts()
                st.success("Forecasts are ready!")
        
        # Step 3: Analysis
        if st.button("3. Run AI Analysis"):
            with st.spinner("🧠 Agents are analyzing historical data and forecasts..."):
                run_analysis()
                st.success("Daily briefing complete!")
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
    st.header("🔍 Exploratory Data Analysis & Forecasts")
    st.write("Visualize the raw data that the AI agents use for their analysis.")

    try:
        conn = get_db_connection()
        st.subheader("Economic Indicators & Forecasts")
        indicators = pd.read_sql_query("SELECT DISTINCT indicator_name FROM singstat_data", conn)['indicator_name'].tolist()
        
        if indicators:
            selected_indicator = st.selectbox("Select an Indicator to Visualize", options=indicators)
            if selected_indicator:
                # Fetch historical data
                df_hist = pd.read_sql_query(f"SELECT record_date, value FROM singstat_data WHERE indicator_name = '{selected_indicator}'", conn, parse_dates=['record_date'])
                df_hist.rename(columns={'value': 'Historical'}, inplace=True)
                
                # Fetch forecast data
                df_fcst = pd.read_sql_query(f"SELECT forecast_date, predicted_value FROM indicator_forecasts WHERE indicator_name = '{selected_indicator}'", conn, parse_dates=['forecast_date'])
                df_fcst.rename(columns={'predicted_value': 'Forecast', 'forecast_date': 'record_date'}, inplace=True)
                
                # Combine for plotting
                df_plot = pd.merge(df_hist, df_fcst, on='record_date', how='outer').set_index('record_date')
                
                st.line_chart(df_plot)
                st.dataframe(df_plot.sort_index(ascending=False).head(10))
                
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
