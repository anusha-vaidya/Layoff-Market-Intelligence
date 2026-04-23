"""
MODULE: Ingestion Engine
DESCRIPTION: This script handles the automated extraction of news signals and market data, storing them in a local DuckDB instance.
"""

import requests
import pandas as pd
import yfinance as yf
import duckdb
import os
from datetime import datetime

# FIX: Absolute path ensuring the folder exists in the GitHub Runner environment
DB_DIR = "data"
DB_PATH = os.path.join(DB_DIR, "layoff_intelligence.db")

def initialize_database():
    if not os.path.exists(DB_DIR):
        os.makedirs(DB_DIR)
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS layoffs_raw (timestamp TIMESTAMP, source_url VARCHAR, headline VARCHAR, sentiment_score FLOAT);")
    con.execute("CREATE TABLE IF NOT EXISTS market_data (timestamp TIMESTAMP, ticker VARCHAR, price FLOAT, volume BIGINT);")
    con.close()

def fetch_layoff_news():
    # Targets AI-related layoff signals
    url = 'https://api.gdeltproject.org/api/v2/doc/doc?query=("layoff" OR "job cuts") AND "AI"&mode=artlist&format=json&timespan=1d'
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status() # Fix: Stops script if API is down
        data = response.json()
        articles = data.get('articles', [])
        con = duckdb.connect(DB_PATH)
        for art in articles:
            con.execute("INSERT INTO layoffs_raw SELECT ?, ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM layoffs_raw WHERE source_url = ?)", 
                        [datetime.now(), art['url'], art['title'], 0.0, art['url']])
        con.close()
    except Exception as e:
        print(f"News fetch failed: {e}") # Non-blocking error

def fetch_market_context():
    con = duckdb.connect(DB_PATH)
    for ticker in ["NVDA", "MSFT", "GOOGL", "AMZN", "META"]:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1d")
            if not hist.empty:
                con.execute("INSERT INTO market_data VALUES (?, ?, ?, ?)", 
                            [datetime.now(), ticker, hist['Close'].iloc[-1], hist['Volume'].iloc[-1]])
        except Exception as e:
            print(f"Market fetch failed for {ticker}: {e}")
    con.close()

if __name__ == "__main__":
    initialize_database()
    fetch_layoff_news()
    fetch_market_context()
