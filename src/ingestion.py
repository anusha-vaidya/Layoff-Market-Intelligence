"""
PROJECT: Layoff Market Intelligence
MODULE: Ingestion Engine
DESCRIPTION: This script handles the automated extraction of news signals 
             and market data, storing them in a local DuckDB instance.
"""

import requests
import pandas as pd
import yfinance as yf
import duckdb
from datetime import datetime

# --- CONFIGURATION & GLOBAL SETTINGS ---
# We use a local DuckDB file to keep the project portable and easy to clone.
DB_PATH = "data/layoff_intelligence.db"

# This query targets specific "Substitution" keywords relevant to the job market.
SEARCH_KEYWORDS = '("layoff" OR "job cuts" OR "headcount reduction") AND "AI"'

# Tracking the 'AI Giants' to see if their hiring/firing cycles lead the broader market.
MARKET_TICKERS = ["NVDA", "MSFT", "GOOGL", "AMZN", "META"]

def initialize_database():
  # Connect to DuckDB
  con = duckdb.connect(DB_PATH)
  con.execute("""
      CREATE TABLE IF NOT EXISTS layoffs_raw (
          timestamp TIMESTAMP,
          source_url VARCHAR,
          headline VARCHAR,
          sentiment_score FLOAT );
  """)

  con.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            timestamp TIMESTAMP,
            ticker VARCHAR,
            price FLOAT,
            volume BIGINT
        );
    """)

  con.close()
  print("Database and tables initialized successfully")

def fetch_layoff_news():
  """
    Extracts global news signals from Global Database of Events, Language, and Tone(GDELT). GDELT is used because it provides a free, high-volume alternative to expensive news APIs.
    """
  print(f"[{datetime.now()}] Searching for AI-related layoff signals.....")
  url = f"https://api.gdeltproject.org/api/v2/doc/doc?query={SEARCH_KEYWORDS}&mode=artlist&format=json&timespan=1d"

  try:
    response = requests.get(url)
    data = response.json()
    articles = data.get('articles', [])
    con = duckdb.connect(DB_PATH)
  
    # We use a 'Deduplication' logic here to ensure we don't count the same news twice
    for art in articles:
      con.execute("""
          INSERT INTO layoffs_raw
          SELECT ?, ?, ?, ?
          WHERE NOT EXISTS(SELECT 1 FROM layoffs_raw WHERE source_url = ?)
          """, [datetime.now(), art['url'], art['title'], 0.0, art['url']]

    con.close()
    print(f"Ingested {len(articles} unique news headlines")

  #Catching and logging errors so the pipeline doesn't crash
  except Exception as e:
    print(f"Critical issue during news ingestion: {e}")

def  fetch_market_context():
  print(f"[{datetime.now()}] Syncing market performance for AI leaders... ")
  con = duckdb.connect(DB_PATH)

  for ticker in MARKET_TICKERS:
    try:
      stock = yf.Ticker(ticker)
      hist = stock.history(period="1d")  #Fetching the most recent 1-day closing
      if not hist.empty:
        price = hist['Close'].iloc[-1]
        vol = hist['Volume'].iloc[-1]
        con.execute("INSERT INTO market_data VALUES (?, ?, ?, ?)", [datetime.now(), ticker, price, vol])
    except Exception as e:
      print(f"Could not fetch data for {ticker}: {e}")

  con.close()
  print("Market data sync complete")

if __main__ == "__main__":
  initialize_database()
  fetch_layoff_news()
  fetch_market_context()


