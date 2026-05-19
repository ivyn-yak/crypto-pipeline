from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

from schema import crypto_schema
from transformations import parse_kafka_stream
from config import *

def main():
    spark = SparkSession.builder.appName("gold_alerts").getOrCreate()

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
    )

    # Simple alert rule: price spike
    alerts = parsed_df.filter(col("price") > 80000)

    query = (
        alerts.writeStream
        .format("parquet")
        .option("path", GOLD_PATH)
        .option("checkpointLocation", CHECKPOINT_GOLD)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()