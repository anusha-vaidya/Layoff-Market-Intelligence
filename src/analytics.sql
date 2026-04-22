/* 
MODULE: Analytics and Signal Detection
DESCRIPTION: This script transforms raw news and market data into actionable intelligence, identifying corrections between AI news and market values */

CREATE OR REPLACE VIEW v_clean_signals AS
SELECT
    CAST(timestamp AS Date) as event_date,
    LOWER(headline) AS clean_headline,
    source_url,
    -- Simple logic to flag if the news is specifically about 'restructuring' or 'efficiency'
    CASE
        WHEN headline ILIKE '%restructure%' OR headline ILIKE '%efficiency%' THEN TRUE
        ELSE FALSE
    END AS is_structural_shift
FROM layoffs_raw;

-- We use Window Functions (LAG) to see how stock prices changed since the last record.
CREATE OR REPLACE VIEW v_market_momentum AS
WITH price_changes AS (
  SELECT
      timestamp,
      ticker,
      price,
      LAG(price) OVER (PARTITION BY ticker ORDER BY timestamp) AS previous_price
  FROM market_data
)

SELECT *,
    (price - previous_price) / previous_price * 100 AS daily_return_pct    -- Calculate the percentage change (Velocity)
FROM price_changes;

--The "Intelligence" Join
CREATE OR REPLACE VIEW v_ai_impact_analysis AS
SELECT
      s.event_date,
      m.ticker,
      m.daily_return_pct,
      COUNT(s.source_url) OVER (PARTITION BY s.event_ate) AS news_volume
FROM v_clean_signals s
JOIN v_market_momentum m ON s.event_date = CAST(m.timestamp AS DATE)
WHERE s.is_structural_shift = TRUE;
