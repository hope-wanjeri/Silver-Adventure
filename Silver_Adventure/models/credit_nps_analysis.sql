{{ config(materialized='table') }}

WITH latest_nps AS (
    -- Linkage logic: Ensure we only use the most recent survey per loan
    SELECT * FROM (
        SELECT 
            Loan_Id,
            NPS_Score,
            NPS_Reason,
            NPS_Phone_Lock,
            Submitted_at,
            ROW_NUMBER() OVER (PARTITION BY Loan_Id ORDER BY Submitted_at DESC) as latest_rank
        FROM {{ source('sales_credit_data', 'nps_data') }}
    ) WHERE latest_rank = 1
),

credit_base AS (
    -- Credit Metric calculation: Analyze loan pricing and return status
    SELECT 
        loan_id,
        SALE_ID,
        SALE_DATE,
        SALE_TYPE,
        PRODUCT_NAME,
        CASH_PRICE,
        LOAN_PRICE,
        -- Calculate the "Credit Cost" (How much extra the client pays for the loan)
        (LOAN_PRICE - CASH_PRICE) AS credit_financing_cost,
        -- Calculate the percentage markup for the loan
        CASE 
            WHEN CASH_PRICE > 0 THEN ((LOAN_PRICE - CASH_PRICE) / CASH_PRICE) * 100 
            ELSE 0 
        END AS financing_markup_pct,
        RETURNED,
        RETURN_DATE
    FROM {{ source('sales_credit_data','nps_data') }}
)

SELECT 
    c.*,
    n.NPS_Score,
    -- Analysis Logic: Categorize NPS into Promoter/Passive/Detractor
    CASE 
        WHEN n.NPS_Score >= 9 THEN 'Promoter'
        WHEN n.NPS_Score <= 6 THEN 'Detractor'
        ELSE 'Passive' 
    END AS nps_category,
    n.NPS_Phone_Lock,
    -- Linkage Metric: Identify if pricing/lock issues impact sentiment
    CASE 
        WHEN n.NPS_Score < 7 AND c.financing_markup_pct > 30 THEN 'High Cost / Low Satisfaction'
        WHEN n.NPS_Phone_Lock = 'Yes' AND n.NPS_Score < 5 THEN 'Locking Issue Detractor'
        ELSE 'General'
    END AS customer_segmentation
FROM credit_base c
LEFT JOIN latest_nps n ON c.loan_id = n.Loan_Id
