from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col

from schema import crypto_schema
from transformations import parse_kafka_stream
from config import *

def main():
    spark = SparkSession.builder.appName("bronze_ingest").getOrCreate()

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

    # query = (
    #     parsed_df.writeStream
    #     .format("parquet")
    #     .option("path", BRONZE_PATH)
    #     .option("checkpointLocation", CHECKPOINT_BRONZE)
    #     .outputMode("append")
    #     .start()
    # )

    query = (
        parsed_df.writeStream
        .format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_BRONZE)
        .outputMode("append")
        .trigger(processingTime="5 seconds")   # 🔥 force micro-batch
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()