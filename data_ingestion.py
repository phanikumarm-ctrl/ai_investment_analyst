# data_ingestion.py
import sqlite3
import yfinance as yf
import requests
import pandas as pd
from datetime import datetime, timedelta
from config import DATABASE_NAME, SINGSTAT_API_KEY

def fetch_and_store_singstat_data(indicator_name, resource_id):
    """Fetches a specific time series from SingStat API and stores it."""
    # This function remains the same as before
    if not SINGSTAT_API_KEY:
        print(f"Skipping {indicator_name}: SINGSTAT_API_KEY not found in .env file.")
        return

    print(f"Fetching {indicator_name} from SingStat...")
    api_url = "https://tablebuilder.singstat.gov.sg/api/table/tabledata"
    
    to_date = datetime.now()
    from_date = to_date - timedelta(days=36*30)
    
    payload = { "resourceId": resource_id, "searchCriteria": { "timeFrom": from_date.strftime('%Y%m'), "timeTo": to_date.strftime('%Y%m') } }
    headers = {"Content-Type": "application/json", "api-key": SINGSTAT_API_KEY}

    try:
        response = requests.post(api_url, json=payload, headers=headers)
        response.raise_for_status()
        
        records = []
        for row in response.json()['Data']['row']:
            date_str = row['key']
            value = row['columns'][0]['value']
            record_date = datetime.strptime(date_str, "%Y %b").date()
            if value.replace('.', '', 1).isdigit():
                 records.append({ "indicator_name": indicator_name, "record_date": record_date, "value": float(value) })

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
    except Exception as e:
        print(f"An unexpected error occurred for {indicator_name}: {e}")


def store_sgx_data(tickers):
    # This function remains the same as before, but we'll call it with more tickers.
    print(f"Fetching SGX stock data for: {tickers}")
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
    # This function remains the same.
    print("Storing mock news data...")
    conn = sqlite3.connect(DATABASE_NAME)
    # (Code for mock news data is unchanged)
    conn.close()

def run_ingestion():
    """Runs all data ingestion functions with the expanded list of indicators."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM sgx_stocks_daily;")
    cursor.execute("DELETE FROM singstat_data;")
    cursor.execute("DELETE FROM unstructured_news;")
    conn.commit()
    conn.close()
    
    # --- Expanded List of SingStat Indicators ---
    # NOTE: These resource IDs are examples. You should verify them on the SingStat website.
    singstat_indicators = {
        # Economy & Prices
        "CPI_All_Items": "M212881",
        "GDP_Quarterly": "M015651", # Example ID for Gross Domestic Product
        "Exchange_Rate_USD": "M050111", # Example ID for SGD per USD
        # Industry Sectorwise
        "Manufacturing_Output": "M015121",
        "Retail_Sales_Index": "M615331", # Example ID for Retail Sales
        "Wholesale_Trade_Index": "M353221", # Example ID for Wholesale Trade
        # Trade and Investment
        "International_Trade_Total": "M082121",
        "Foreign_Direct_Investment": "M085181", # Example ID for FDI
        "Investment_Commitments_Mfg": "M015091" # Example ID for Investment Commitments
    }

    for name, resource_id in singstat_indicators.items():
        fetch_and_store_singstat_data(name, resource_id)
        
    # --- Expanded List of yfinance Tickers ---
    sgx_tickers = [
        "C6L.SI",  # Singapore Airlines (Aviation, Consumer)
        "D05.SI",  # DBS Bank (Banking, Finance)
        "O39.SI",  # OCBC Bank (Banking, Finance)
        "U11.SI",  # UOB Bank (Banking, Finance)
        "Z74.SI",  # Singtel (Telecommunications)
        "A17U.SI", # CapitaLand Ascendas REIT (Industrial, REIT)
        "M44U.SI"  # Mapletree Industrial Trust (Industrial, REIT)
    ]
    store_sgx_data(sgx_tickers)
    
    store_mock_news_data() # This remains the same
    
    print("\nExpanded data ingestion complete.")
    
