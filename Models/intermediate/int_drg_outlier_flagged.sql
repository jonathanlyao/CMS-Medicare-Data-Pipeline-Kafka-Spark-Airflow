-- Intermediate layer: flag DRGs where charge ratio is abnormally high. 
-- Outlier definition: avg_charge_ratio > 2 standard deviations above the mean. 

with base as (
    SELECT * FROM {{ ref('stg_drg_charge_analysis') }}
), 

stats as (
    SELECT 
        AVG(avg_charge_ratio)           AS mean_ratio, 
        STDDEV(avg_charge_ratio)        AS stddev_ratio
    FROM base
),

flagged as (
    SELECT
        b.drg_code, 
        b.drg_description, 
        b.hospital_count,
        b.avg_submitted_charge,
        b.avg_medicare_payment,
        b.avg_total_payment,
        b.avg_charge_ratio,
        b.batch_updated_at,
        s.mean_ratio,
        s.stddev_ratio,

        -- dollars left on the table per discharge (charge vs actual payment)
        ROUND(b.avg_submitted_charge - b.avg_medicare_payment, 2) AS avg_charge_gap, 

        -- outlier flag: ratio more than 2 std devs above mean
        CASE
            WHEN b.avg_charge_ratio > (s.mean_ratio + 2 * s.stddev_ratio)
            THEN TRUE
            ELSE FALSE
        END                             AS is_charge_outlier, 

        -- severtiy tier for dashboard bucketing
        CASE
            WHEN b.avg_charge_ratio >= 15 THEN 'Critical'
            WHEN b.avg_charge_ratio >= 10 THEN 'High'
            WHEN b.avg_charge_ratio >= 5 THEN 'Medium'
            ELSE 'Normal'
        END                             AS charge_severity
    
    FROM base AS b
    CROSS JOIN stats AS s


)

SELECT * FROM flagged