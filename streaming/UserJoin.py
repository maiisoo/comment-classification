from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import col, from_json, struct, window
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

# Hàm tạo schema
def createSchema(columns):
    fields = []
    for column in columns:
        field = StructField(column, StringType(), False)
        fields.append(field)
    schema = StructType(fields)
    return schema

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("Join and Send to Kafka") \
    .getOrCreate()

# Đọc dữ liệu từ Kafka topic "test1"
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test1") \
    .load()

# Tạo schema cho dữ liệu comment
commentSchema = createSchema(columns=["user_id", "platform", "timestamp", "text", "post_id", "topic"])

# Chuyển đổi giá trị từ Kafka thành cấu trúc DataFrame
commentDF = df.withColumn("value", df["value"].cast(StringType())) \
    .withColumn("value", from_json(col("value"), commentSchema).alias("value")) \
    .selectExpr("value.*")

# Chuyển kiểu dữ liệu của cột timestamp sang TimestampType
commentDF = commentDF.withColumn("timestamp", commentDF["timestamp"].cast(TimestampType()))

# Tạo watermark với thời gian là 10 phút
commentDF = commentDF.withWatermark("timestamp", "10 minutes")

# Đọc dữ liệu user từ tệp CSV
userSchema = createSchema(columns=["User_id","Name","Email","Gender","Birthday","Location","Phone_number","Registration_date"])
userDF = spark.read.csv("hdfs://master1:9000/user/dis/data_prj/users.csv", header=True, schema=userSchema)

# Thực hiện join giữa dữ liệu user và dữ liệu comment
joinDF = userDF.join(commentDF, userDF["User_id"] == commentDF["user_id"], "inner")
joinDF = joinDF.drop(joinDF["user_id"])

# Ghi kết quả vào Kafka topic "test2"
# query = joinDF \
#     .selectExpr("to_json(struct(*)) AS value") \
#     .writeStream \
#     .format("kafka") \
#     .outputMode("append") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("topic", "test") \
#     .start()

# # Chạy query
# query.awaitTermination()
