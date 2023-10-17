from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType
import time

spark = SparkSession.builder.appName("SQLChangeTracker").getOrCreate()

url = "jdbc:sqlserver://localhost:1433;databaseName=YourDBName"
properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

your_schema = StructType() \
    .add("id", StringType()) \
    .add("column2", StringType()) \
    .add("LastModified", StringType())

while True:
    query = "(SELECT * FROM YourTable WHERE LastModified > 'your_last_timestamp') as tmp"
    df = spark.read.jdbc(url, query, properties=properties)

    if df.count() > 0:
        df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
          .write \
          .format("kafka") \
          .option("kafka.bootstrap.servers", "localhost:9092") \
          .option("topic", "sql_changes") \
          .save()

        kafkaStream = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "sql_changes") \
            .load()

        df_kafka = kafkaStream.selectExpr("CAST(key AS STRING)", "from_json(CAST(value AS STRING), your_schema) as data") \
            .select("data.*")

        df_kafka.write.jdbc(url, "YourOtherTable", mode="append", properties=properties)

    time.sleep(300)
