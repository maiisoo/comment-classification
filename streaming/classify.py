from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .master("") \
    .appName("classifier") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka_producer") \
    .option("kafka_producer.bootstrap.servers", "") \
    .option("subscribe", "test") \
    .option("includeHeaders", "true") \
    .load()


columns = ["user_id", "platform", "timestamp", "text", "post_id", "topic"]
fields = []
for column in columns:
    field = StructField(column, StringType(), False)
    fields.append(field)
schema = StructType(fields)

df = df.filter(col("topic") == "test")
df = df.withColumn("value", df["value"].cast(StringType()))
df = df.withColumn("value", from_json(col("value"), schema).alias("value")) \
        .selectExpr("value.*")


# Load trained model
loaded_model = PipelineModel.load("")

predictions = loaded_model.transform(df.select("text"))

# Display
predictions_query = predictions \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

predictions_query.awaitTermination()
