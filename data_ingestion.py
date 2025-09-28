# data_ingestion.py
import sqlite3
import yfinance as yf
import requests # New import
import pandas as pd
from datetime import datetime, timedelta
from config import DATABASE_NAME, SINGSTAT_API_KEY # Import API key

def fetch_and_store_singstat_data(indicator_name, resource_id):
    """Fetches a specific time series from SingStat API and stores it."""
    if not SINGSTAT_API_KEY:
        print(f"Skipping {indicator_name}: SINGSTAT_API_KEY not found in .env file.")
        return

    print(f"Fetching {indicator_name} from SingStat...")
    api_url = "https://tablebuilder.singstat.gov.sg/api/table/tabledata"
    
    # Calculate date range for the last 36 months
    to_date = datetime.now()
    from_date = to_date - timedelta(days=36*30)
    
    payload = {
        "resourceId": resource_id,
        "searchCriteria": {
            "timeFrom": from_date.strftime('%Y%m'),
            "timeTo": to_date.strftime('%Y%m')
        }
    }
    headers = {"Content-Type": "application/json", "api-key": SINGSTAT_API_KEY}

    try:
        response = requests.post(api_url, json=payload, headers=headers)
        response.raise_for_status()
        
        records = []
        # The API response is nested; we need to parse it carefully
        for row in response.json()['Data']['row']:
            # The 'key' is the date, e.g., "2024 Sep"
            date_str = row['key']
            # The 'columns' list contains the value
            value = row['columns'][0]['value']
            
            # Convert date string to a proper date format
            record_date = datetime.strptime(date_str, "%Y %b").date()
            if value.replace('.', '', 1).isdigit(): # Check if value is a valid number
                 records.append({
                    "indicator_name": indicator_name,
                    "record_date": record_date,
                    "value": float(value)
                })

        if not records:
            print(f"No data found for {indicator_name}.")
            return

        df = pd.DataFrame(records)
        conn = sqlite3.connect(DATABASE_NAME)
        df.to_sql('singstat_data', conn, if_exists='append', index=False)
        conn.close()
        print(f"Successfully stored {len(df)} records for {indicator_name}.")

    except requests.exceptions.HTTPError as e:
        print(f"Error fetching data for {indicator_name}: {e.response.text}")
    except (KeyError, IndexError, TypeError) as e:
        print(f"Error parsing API response for {indicator_name}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred for {indicator_name}: {e}")


def store_sgx_data(tickers):
    # This function remains the same as before
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
                final_data = stock_data[['ticker', 'record_date', 'close_price', 'volume']]
                final_data.to_sql('sgx_stocks_daily', conn, if_exists='append', index=False)
                print(f"Data for {ticker} stored.")
        except Exception as e:
            print(f"Could not fetch data for {ticker}: {e}")
    conn.close()


def store_mock_news_data():
    # This function remains the same as before
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
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    # Clear existing data to avoid duplicates on each run
    cursor.execute("DELETE FROM sgx_stocks_daily;")
    cursor.execute("DELETE FROM singstat_data;")
    cursor.execute("DELETE FROM unstructured_news;")
    conn.commit()
    conn.close()
    
    # --- Live SingStat API Calls ---
    # NOTE: These resource IDs are examples and may change.
    # You can find them by exploring tables on the SingStat website.
    fetch_and_store_singstat_data("CPI_All_Items", "M212881")
    fetch_and_store_singstat_data("Manufacturing_Output", "M015121")
    fetch_and_store_singstat_data("International_Trade_Total", "M082121")
    
    sgx_tickers = ["C6L.SI", "D05.SI", "O39.SI"] # SIA, DBS, OCBC
    store_sgx_data(sgx_tickers)
    store_mock_news_data()
    print("\nData ingestion complete.")
    
