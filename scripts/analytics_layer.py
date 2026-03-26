from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, avg, desc
import os

# 1. Inisialisasi Spark
spark = SparkSession.builder.appName("AnalyticsLayer").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 2. Buat folder untuk hasil dashboard
if not os.path.exists("data/serving"):
    os.makedirs("data/serving")

print("Membaca data Parquet yang sudah bersih...")
df_clean = spark.read.parquet("data/clean/parquet/")

# 3. Hitung KPI 1: Total Revenue
total_revenue = df_clean.agg(_sum("total_amount").alias("total_revenue"))
total_revenue.write.mode("overwrite").option("header", True).csv("data/serving/total_revenue")

# 4. Hitung KPI 2: Top 10 Products
top_products = df_clean.groupBy("product").agg(_sum("quantity").alias("total_quantity")).orderBy(desc("total_quantity")).limit(10)
top_products.write.mode("overwrite").option("header", True).csv("data/serving/top_products")

# 5. Hitung KPI 3: Revenue per Category
category_revenue = df_clean.groupBy("category").agg(_sum("total_amount").alias("category_revenue")).orderBy(desc("category_revenue"))
category_revenue.write.mode("overwrite").option("header", True).csv("data/serving/category_revenue")

print("ANALYTICS LAYER COMPLETED SUCCESSFULLY!")
spark.stop()