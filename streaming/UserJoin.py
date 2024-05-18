from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from pyspark.ml import PipelineModel


# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 UserJoin.py
# /opt/spark/bin/spark-submit --master spark://10.10.28.20:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 UserJoin.py

# Hàm tạo schema
def createSchema(columns):
    fields = []
    for column in columns:
        field = StructField(column, StringType(), False)
        fields.append(field)
    schema = StructType(fields)
    return schema


# Mapping label
def prediction_to_label(prediction):
    emotion_labels = {0.0: 'sadness', 1.0: 'joy', 2.0: 'anger', 3.0: 'fear', 4.0: 'surprise', 5.0: 'love'}
    return emotion_labels[prediction]


# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("UserJoin") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Đọc dữ liệu từ Kafka bằng cách sử dụng readStream
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .load()

# Chuyển đổi dữ liệu từ JSON thành DataFrame
commentDF = df.selectExpr("CAST(value AS STRING)") \
    .selectExpr(
    "from_json(value, 'user_id STRING, platform STRING, timestamp STRING, text STRING, post_id STRING, topic STRING') AS data") \
    .select("data.*")
commentDF = commentDF.withColumn("timestamp", commentDF["timestamp"].cast(TimestampType()))

loaded_model = PipelineModel.load("hdfs://master1:9000/user/dis/model_dir")
prediction_to_label_udf = udf(prediction_to_label, StringType())
commentDF = loaded_model.transform(commentDF)
commentDF = commentDF.withColumn("label", prediction_to_label_udf(commentDF["prediction"]))
commentDF = commentDF.select("user_id", "platform", "text", "timestamp", "post_id", "topic", "label")

# # Đọc dữ liệu user từ tệp CSV
userSchema = createSchema(
    columns=["User_id", "Name", "Email", "Gender", "Birthday", "Location", "Phone_number", "Registration_date"])
userDF = spark.read.csv("hdfs://master1:9000/user/dis/data_prj/users.csv", header=True, schema=userSchema)

commentDF = commentDF.withWatermark("timestamp", "10 minutes")
commentDF = commentDF.withColumnRenamed("user_id", "comment_user_id")

loaded_model = PipelineModel.load("hdfs://master1:9000/user/dis/model")
prediction_to_label_udf = udf(prediction_to_label, StringType())
commentDF = loaded_model.transform(commentDF)
commentDF = commentDF.withColumn("label", prediction_to_label_udf(commentDF["prediction"]))
commentDF = commentDF.select("user_id", "platform", "text", "timestamp", "post_id", "topic", "label")

# Thực hiện join giữa dữ liệu user và dữ liệu comment
# joinDF = userDF.join(commentDF, userDF["User_id"] == commentDF["user_id"], "inner")
joinDF = userDF.join(commentDF, userDF["User_id"] == commentDF["comment_user_id"], "inner")
joinDF = joinDF.drop(joinDF["user_id"])
# In kết quả ra console
query_console = joinDF \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Chờ cho đến khi query kết thúc
query_console.awaitTermination()


def write_to_table(df, epoc_id):
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="comments", keyspace="group_1") \
            .mode("append") \
            .save()

        print("Write real time comment to Cassandra table successfully!")
    except Exception as e:
        print(f"Error while writing to Cassandra:{e}")


# Ghi kết quả vào cassadra DB
query_db = joinDF.writeStream \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .foreachBatch(write_to_table) \
    .start()

query_db.awaitTermination()


# Đóng SparkSession
spark.stop()
