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

