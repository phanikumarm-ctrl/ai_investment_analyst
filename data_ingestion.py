# data_ingestion.py
import sqlite3
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
from config import DATABASE_NAME

def store_sgx_data(tickers):
    """Fetches and stores daily stock data for a list of SGX tickers."""
    print("Fetching SGX stock data...")
    conn = sqlite3.connect(DATABASE_NAME)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    for ticker in tickers:
        try:
            stock_data = yf.download(ticker, start=start_date, end=end_date)
            if not stock_data.empty:
                stock_data = stock_data.reset_index()
                stock_data.rename(columns={'Date': 'record_date', 'Close': 'close_price', 'Volume': 'volume'}, inplace=True)
                stock_data['ticker'] = ticker
                # Keep only necessary columns
                final_data = stock_data[['ticker', 'record_date', 'close_price', 'volume']]
                final_data.to_sql('sgx_stocks_daily', conn, if_exists='append', index=False)
                print(f"Data for {ticker} stored.")
        except Exception as e:
            print(f"Could not fetch data for {ticker}: {e}")
    conn.close()

def store_mock_economic_data():
    """Stores mock economic data for demonstration."""
    print("Storing mock economic data...")
    conn = sqlite3.connect(DATABASE_NAME)
    # Creating a dummy DataFrame for CPI and Manufacturing Output
    dates = pd.to_datetime(pd.date_range(end=datetime.now(), periods=12, freq='MS'))
    cpi_data = pd.DataFrame({
        'indicator_name': 'CPI_All_Items',
        'record_date': dates,
        'value': [102.5, 102.8, 103.1, 103.0, 103.5, 103.6, 103.8, 104.0, 104.2, 104.1, 104.5, 104.8]
    })
    mfg_data = pd.DataFrame({
        'indicator_name': 'Manufacturing_Output',
        'record_date': dates,
        'value': [98.5, 99.2, 99.8, 100.5, 100.2, 101.1, 101.5, 101.3, 102.0, 102.5, 102.3, 103.1]
    })
    cpi_data.to_sql('singstat_data', conn, if_exists='append', index=False)
    mfg_data.to_sql('singstat_data', conn, if_exists='append', index=False)
    conn.close()

def store_mock_news_data():
    """Stores mock news articles for demonstration."""
    print("Storing mock news data...")
    conn = sqlite3.connect(DATABASE_NAME)
    news_data = pd.DataFrame([
        {'source': 'Business Times', 'published_date': (datetime.now() - timedelta(days=10)).date(), 'content': 'Singapore Airlines (SIA) reports record passenger numbers for the last quarter, citing strong travel demand.'},
        {'source': 'Reuters', 'published_date': (datetime.now() - timedelta(days=5)).date(), 'content': 'DBS Bank announces new digital initiatives aimed at expanding its wealth management services across Southeast Asia.'},
        {'source': 'MAS', 'published_date': (datetime.now() - timedelta(days=3)).date(), 'content': 'The Monetary Authority of Singapore signals a tightening of monetary policy in response to rising core inflation metrics.'},
        {'source': 'Business Times', 'published_date': (datetime.now() - timedelta(days=1)).date(), 'content': 'Local manufacturing output sees a slight increase, driven by the electronics sector, but experts remain cautious.'}
    ])
    news_data.to_sql('unstructured_news', conn, if_exists='append', index=False)
    conn.close()

def run_ingestion():
    """Runs all data ingestion functions."""
    # Note: Clear existing data to avoid duplicates in this simple setup
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM sgx_stocks_daily;")
    cursor.execute("DELETE FROM singstat_data;")
    cursor.execute("DELETE FROM unstructured_news;")
    conn.commit()
    conn.close()
    
    sgx_tickers = ["C6L.SI", "D05.SI", "O39.SI"] # SIA, DBS, OCBC
    store_sgx_data(sgx_tickers)
    store_mock_economic_data()
    store_mock_news_data()
    print("Data ingestion complete.")
