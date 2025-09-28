# database_setup.py
import sqlite3
from config import DATABASE_NAME

def create_connection():
    """Create a database connection to the SQLite database."""
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        return conn
    except sqlite3.Error as e:
        print(e)
    return conn

def create_tables():
    """Create the necessary tables for the application."""
    conn = create_connection()
    if conn is not None:
        try:
            c = conn.cursor()
            
            # For storing structured economic data from SingStat
            c.execute("""
                CREATE TABLE IF NOT EXISTS singstat_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    indicator_name TEXT NOT NULL,
                    record_date DATE NOT NULL,
                    value REAL,
                    UNIQUE(indicator_name, record_date)
                );
            """)

            # For storing daily stock data
            c.execute("""
                CREATE TABLE IF NOT EXISTS sgx_stocks_daily (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    record_date DATE NOT NULL,
                    close_price REAL,
                    volume INTEGER,
                    UNIQUE(ticker, record_date)
                );
            """)
            
            # For storing unstructured news/reports
            c.execute("""
                CREATE TABLE IF NOT EXISTS unstructured_news (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    published_date DATE NOT NULL,
                    content TEXT NOT NULL
                );
            """)

            # For storing the agent's long-term memory
            c.execute("""
                CREATE TABLE IF NOT EXISTS memory_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    log_date DATE UNIQUE NOT NULL,
                    summary TEXT NOT NULL
                );
            """)

            # For storing the final daily briefings
            c.execute("""
                CREATE TABLE IF NOT EXISTS briefings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    briefing_date DATE UNIQUE NOT NULL,
                    content TEXT NOT NULL
                );
            """)

            conn.commit()
            print("Database and tables created successfully.")
        except sqlite3.Error as e:
            print(f"Error creating tables: {e}")
        finally:
            conn.close()
    else:
        print("Error! cannot create the database connection.")

if __name__ == '__main__':
    create_tables()
