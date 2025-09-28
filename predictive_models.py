# predictive_models.py
import sqlite3
import pandas as pd
from prophet import Prophet
from config import DATABASE_NAME

def generate_forecasts():
    """
    Trains a time-series model for each economic indicator and stores the forecasts.
    """
    print("Generating ML forecasts for economic indicators...")
    conn = sqlite3.connect(DATABASE_NAME)
    
    # Clear old forecasts
    conn.execute("DELETE FROM indicator_forecasts;")
    conn.commit()

    # Get the list of unique indicators to forecast
    indicators_df = pd.read_sql_query("SELECT DISTINCT indicator_name FROM singstat_data", conn)
    indicators = indicators_df['indicator_name'].tolist()

    all_forecasts = []

    for indicator in indicators:
        print(f"  - Forecasting for {indicator}...")
        # Fetch historical data for the current indicator
        history_df = pd.read_sql_query(
            f"SELECT record_date, value FROM singstat_data WHERE indicator_name = '{indicator}'",
            conn,
            parse_dates=['record_date']
        )
        
        # Prophet requires columns to be named 'ds' (datestamp) and 'y' (value)
        history_df.rename(columns={'record_date': 'ds', 'value': 'y'}, inplace=True)
        
        if len(history_df) < 12:
            print(f"    - Skipping {indicator}: Insufficient historical data (need at least 12 points).")
            continue

        try:
            # Initialize and train the Prophet model
            model = Prophet(yearly_seasonality=True, daily_seasonality=False)
            model.fit(history_df)

            # Create a future dataframe to predict on (next 3 months)
            future = model.make_future_dataframe(periods=90)
            forecast = model.predict(future)
            
            # Extract the forecast data and format for storage
            forecast['indicator_name'] = indicator
            forecast_to_store = forecast[['indicator_name', 'ds', 'yhat']].copy()
            forecast_to_store.rename(columns={'ds': 'forecast_date', 'yhat': 'predicted_value'}, inplace=True)
            
            # Keep only future predictions
            future_only = forecast_to_store[forecast_to_store['forecast_date'] > history_df['ds'].max()]
            all_forecasts.append(future_only)

        except Exception as e:
            print(f"    - Could not generate forecast for {indicator}: {e}")

    if all_forecasts:
        final_forecast_df = pd.concat(all_forecasts)
        final_forecast_df.to_sql('indicator_forecasts', conn, if_exists='append', index=False)
        print(f"\nSuccessfully generated and stored {len(final_forecast_df)} new forecast points.")
    
    conn.close()

if __name__ == '__main__':
    generate_forecasts()
  
