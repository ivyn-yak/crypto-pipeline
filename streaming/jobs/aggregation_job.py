from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, window, avg, max, min

from schema import crypto_schema
from transformations import parse_kafka_stream
from config import *

def main():
    spark = SparkSession.builder.appName("silver_agg").getOrCreate()

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", KAFKA_TOPIC)
        .load()
    )

    parsed_df = parse_kafka_stream(df, crypto_schema)

    parsed_df = parsed_df.withColumn(
        "event_time",
        to_timestamp(col("event_time"))
    ).filter(col("price") > 0)

    aggregated_df = (
        parsed_df
        .withWatermark("event_time", "10 seconds")  
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("symbol")
        )
        .agg(
            avg("price").alias("avg_price"),
            max("price").alias("max_price"),
            min("price").alias("min_price"),
        )
    )

    query = (
        aggregated_df.writeStream
        .outputMode("append")       
        .format("parquet")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", CHECKPOINT_SILVER)
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()