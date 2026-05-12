-- Staging layer: clean and rename fields from the Spark streaming output. 
-- Source: CMS_MEDICARE.STREAMING.DRG_CHARGE_ANALYSIS

WITH source AS (

    SELECT * FROM {{ source('streaming', 'drg_charge_analysis') }}
), 

renamed AS (
    SELECT 
        drg_cd                      as drg_code, 
        drg_desc                    as drg_description, 
        hospital_count, 
        avg_charge                  as avg_submitted_charge, 
        avg_medicare_pymt           as avg_medicare_payment, 
        avg_total_pymt              as avg_total_payment, 
        avg_charge_ratio, 
        updated_at                  as batch_updated_at

    FROM source
    WHERE drg_code IS NOT NULL
      AND avg_submitted_charge > 0
      AND avg_medicare_payment > 0
)

SELECT * FROM renamed
