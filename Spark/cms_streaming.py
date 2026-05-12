"""
CMS Medicare Inpatient - Spark Structured Streaming -> Snowflake
Reads CMS data from Kafka, performs DRG-level aggregation,
writes results to Snowflake via foreachBatch.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, avg, count, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType
)
from snowflake_writer import write_to_snowflake

# -- Schema: matches the JSON fields returned by CMS API --
cms_schema = StructType([
    StructField("Rndrng_Prvdr_CCN", StringType()),
    StructField("Rndrng_Prvdr_Org_Name", StringType()),
    StructField("Rndrng_Prvdr_City", StringType()),
    StructField("Rndrng_Prvdr_State_Abrvtn", StringType()),
    StructField("Rndrng_Prvdr_Zip5", StringType()),
    StructField("DRG_Cd", StringType()),
    StructField("DRG_Desc", StringType()),
    StructField("Tot_Dschrgs", StringType()),
    StructField("Avg_Submtd_Cvrd_Chrg", StringType()),
    StructField("Avg_Tot_Pymt_Amt", StringType()),
    StructField("Avg_Mdcr_Pymt_Amt", StringType()),
])


def main():
    spark = (
        SparkSession.builder
        .appName("CMS-Medicare-Streaming")
        .master("local[*]")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # -- Read streaming data from Kafka --
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "cms-inpatient-stream")
        .option("startingOffsets", "earliest")
        .load()
    )

    # -- Parse JSON payload --
    parsed = (
        raw_stream
        .selectExpr("CAST(key AS STRING) as drg_key",
                     "CAST(value AS STRING) as json_str")
        .select(
            col("drg_key"),
            from_json(col("json_str"), cms_schema).alias("data")
        )
        .select("drg_key", "data.*")
        .withColumn("charge", col("Avg_Submtd_Cvrd_Chrg").cast(FloatType()))
        .withColumn("medicare_payment", col("Avg_Mdcr_Pymt_Amt").cast(FloatType()))
        .withColumn("total_payment", col("Avg_Tot_Pymt_Amt").cast(FloatType()))
        .withColumn("discharges", col("Tot_Dschrgs").cast("int"))
    )

    # -- Aggregate: compute key metrics per DRG --
    drg_agg = (
        parsed
        .groupBy("DRG_Cd", "DRG_Desc")
        .agg(
            count("*").alias("hospital_count"),
            spark_round(avg("charge"), 2).alias("avg_charge"),
            spark_round(avg("medicare_payment"), 2).alias("avg_medicare_pymt"),
            spark_round(avg("total_payment"), 2).alias("avg_total_pymt"),
            spark_round(
                avg(col("charge") / col("medicare_payment")), 2
            ).alias("avg_charge_ratio"),
        )
    )

    # -- Write to Snowflake via foreachBatch --
    query = (
        drg_agg.writeStream
        .outputMode("complete")
        .foreachBatch(write_to_snowflake)
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("=" * 60)
    print("CMS Streaming Pipeline -> Snowflake")
    print("Reading from: cms-inpatient-stream")
    print("Writing to:   CMS_MEDICARE.STREAMING.DRG_CHARGE_ANALYSIS")
    print("Trigger:      every 30 seconds")
    print("Press Ctrl+C to stop")
    print("=" * 60)

    query.awaitTermination()


if __name__ == "__main__":
    main()