from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col
from schema import crypto_schema
from transformations import parse_kafka_stream

def main():
    spark = (
        SparkSession.builder
        .appName("KafkaToParquet")
        .getOrCreate()
    )

    raw_df = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "crypto-prices")
            .load()
    ) 

    parsed_df = parse_kafka_stream(raw_df, crypto_schema)

    parsed_df = parsed_df.withColumn(
        "event_time",
        to_timestamp(col("event_time"))
    )

    query = (
        parsed_df.writeStream
            .format("parquet")
            .option("path", "storage/crypto")
            .option("checkpointLocation", "storage/checkpoints/crypto")
            .outputMode("append")
            .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()

