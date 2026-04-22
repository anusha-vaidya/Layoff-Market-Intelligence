📉 Layoff Market Intelligence
Automated sentinel tracking the "AI-Substitution Point" in global labor markets.

This repository uses Python and SQL (DuckDB) to analyze the structural shift in the 2026 labor market. It correlates news-driven layoff sentiment with real-time wage data to predict the "Substitution Point" where AI replaces human workflows.

Architecture

Ingestion: Python engine pulls global news (GDELT) and market data (YFinance).

Analytics: SQL transformation layer using DuckDB Window Functions and CTEs.

Automation: GitHub Actions executes the full pipeline daily at 00:00 UTC.




Ethics & Compliance

Public Data: Only accesses logged-out, publicly available signals.

Privacy: Zero PII collection; data is industry-aggregated.
