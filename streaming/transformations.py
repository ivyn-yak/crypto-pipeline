from pyspark .sql.functions import col, from_json

def parse_kafka_stream(df, schema):
    return (
        df.selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
    )
