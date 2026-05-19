from pyspark.sql.types import StructType, StringType, DoubleType

crypto_schema = (
    StructType() 
        .add("symbol", StringType()) 
        .add("price", DoubleType())
        .add("event_time", StringType())
) 