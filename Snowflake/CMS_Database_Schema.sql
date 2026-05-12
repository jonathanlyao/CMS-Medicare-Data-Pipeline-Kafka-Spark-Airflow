-- Create dedicated database and warehouse for CMS project
CREATE DATABASE IF NOT EXISTS CMS_MEDICARE; 
CREATE WAREHOUSE IF NOT EXISTS CMS_MEDICARE_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE; 

USE DATABASE CMS_MEDICARE; 
USE WAREHOUSE CMS_MEDICARE_WH; 

-- Create schema
CREATE SCHEMA IF NOT EXISTS STREAMING; 

-- Create target table for Spark streaming output
CREATE OR REPLACE TABLE STREAMING.DRG_CHARGE_ANALYSIS (
    DRG_Cd              STRING, 
    DRG_Desc            STRING,
    hospital_count      INT,
    avg_charge          FLOAT,
    avg_medicare_pymt   FLOAT,
    avg_total_pymt      FLOAT,
    avg_charge_ratio    FLOAT,
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
); 

-- Top 20 DRGs with highest charge ratio
SELECT DRG_Cd, DRG_Desc, hospital_count, 
       avg_charge, avg_medicare_pymt, avg_charge_ratio, updated_at
FROM STREAMING.DRG_CHARGE_ANALYSIS
ORDER BY avg_charge_ratio DESC
LIMIT 20;