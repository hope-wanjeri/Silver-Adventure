# Silver-Adventure

# Collections and Credit Analysis Pipeline

This project demonstrates a production-ready dbt pipeline on Databricks to analyze credit metrics and customer sentiment (NPS).

## Data Modeling Strategy
- **Bronze (Sources):** Raw Excel data ingested from Databricks Volumes.
- **Silver (Transformation):** Logic to calculate Days Past Due (DPD) and join latest NPS scores.
- **Gold (Marts):** Final `fct_credit_nps_analysis` table for business intelligence.

## Key Metrics Calculated
- **Credit Financing Cost:** `LOAN_PRICE - CASH_PRICE`
- **Financing Markup %:** Percentage increase for loan customers.
- **NPS Linkage:** Uses `ROW_NUMBER()` to ensure the most recent sentiment is linked to current credit status.

## Assumptions
- NPS surveys are conducted periodically; we use the latest survey relative to the sale date.
- Loan status is determined by the variance between `LOAN_PRICE` and `CASH_PRICE`.
