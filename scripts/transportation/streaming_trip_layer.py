from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 1. Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("SmartTransportationStreaming") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definisi Schema (Agar Spark tahu struktur data JSON kita)
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("distance", DoubleType(), True),
    StructField("fare", LongType(), True),
    StructField("timestamp", StringType(), True)
])

# 3. Read Stream (Membaca file JSON yang masuk ke folder stream_data)
input_path = "../../stream_data/transportation"
raw_stream_df = spark.readStream \
    .schema(schema) \
    .json(input_path)

# 4. Transformasi Ringan: Mengubah string timestamp menjadi format Timestamp asli
processed_df = raw_stream_df.withColumn("event_time", to_timestamp(col("timestamp")))

# 5. Write Stream ke Parquet (Data Lake Serving Layer)
# Format Parquet sangat cepat untuk dibaca oleh Dashboard nanti
output_path = "../../data/serving/transportation"
checkpoint_path = "../../checkpoints/transportation"

query = processed_df.writeStream \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .start()

print(f"[*] Spark Streaming AKTIF...")
print(f"[*] Memproses data dari: {input_path}")
print(f"[*] Menyimpan hasil ke: {output_path}")

query.awaitTermination()