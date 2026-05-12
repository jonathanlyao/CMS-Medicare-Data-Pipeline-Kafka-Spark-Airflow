-- Mart layer: final analytical table for DRG-level spending analysis.
-- Primary use case: identify DRGs where Medicare is significantly
-- underpaying relative to hospital submitted charges.

WITH flagged AS (
    SELECT * FROM {{ ref('int_drg_outlier_flagged') }}
)

SELECT
    drg_code, 
    drg_description, 
    hospital_count,
    avg_submitted_charge,
    avg_medicare_payment,
    avg_total_payment,
    avg_charge_ratio,
    avg_charge_gap,
    is_charge_outlier,
    charge_severity,
    batch_updated_at,

    -- rank DRGs by charge ratio within severity tier
    rank() over (

        PARTITION BY charge_severity
        ORDER BY avg_charge_ratio DESC
    )                                       AS rank_within_severity

    FROM flagged
    ORDER BY avg_charge_ratio DESC