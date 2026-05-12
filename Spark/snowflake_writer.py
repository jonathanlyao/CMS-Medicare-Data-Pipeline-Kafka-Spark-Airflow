"""
Snowflake writer module.
Handles connection and batch inserts to CMS_MEDICARE.STREAMING.DRG_CHARGE_ANALYSIS.
"""

import os
import snowflake.connector

SF_CONFIG = {
    "account": "FZPFTPF-LOB40082",
    "user": "JONATHANSNWFLK",
    "password": os.environ.get("SNOWFLAKE_PASSWORD", ""),
    "database": "CMS_MEDICARE",
    "schema": "STREAMING",
    "warehouse": "CMS_MEDICARE_WH",
}

def write_to_snowflake(batch_df, batch_id):
    """foreachBatch callback: write each micro-batch to Snowflake. """
    rows = batch_df.collect()
    if not rows: 
        print(f"[Batch {batch_id}] Empty batch, skipping.")
        return
    
    conn = snowflake.connector.connect(**SF_CONFIG)
    cursor = conn.cursor()

    try: 
        cursor.execute("TRUNCATE TABLE STREAMING.DRG_CHARGE_ANALYSIS")

        insert_sql = """
            INSERT INTO STREAMING.DRG_CHARGE_ANALYSIS
                (DRG_Cd, DRG_Desc, hospital_count, avg_charge,
                 avg_medicare_pymt, avg_total_pymt, avg_charge_ratio)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        data = [
            (
                row["DRG_Cd"],
                row["DRG_Desc"],
                row["hospital_count"],
                row["avg_charge"],
                row["avg_medicare_pymt"],
                row["avg_total_pymt"],
                row["avg_charge_ratio"],
            )
            for row in rows
        ]

        cursor.executemany(insert_sql, data)
        conn.commit()
        print(f"[Batch {batch_id}] Wrote {len(data)} DRG records to Snowflake.")

    finally: 
        cursor.close()
        conn.close()