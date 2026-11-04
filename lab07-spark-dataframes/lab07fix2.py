import os, subprocess, time
from pyspark.sql import SparkSession
def sh(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print("$ " + cmd + "\n" + r.stdout + r.stderr)

os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]

# Adapt to the Spark and Scala versions of your environment
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .master("local[*]")
    # S3 access
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Iceberg
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
spark
# %%
users_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/users.csv", header=True, inferSchema=True, samplingRatio=0.01, multiLine=True
)
orders_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/orders.csv", header=True, inferSchema=True, samplingRatio=0.01, multiLine=True
)

users_bronze.printSchema()
users_bronze.show(3, truncate=False)
# %%
from pyspark.sql import functions as F

users_typed = (
    users_bronze
    .select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)

stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
)

stg_users = stg_users.withColumn(
    "city", F.when(F.col("city") == "", None).otherwise(F.col("city"))
)

stg_users.show(5, truncate=False)
# %%
from pyspark.sql.window import Window

orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)

print("Original:", orders_typed.count())
# %%
duplicates = orders_typed.limit(3)
orders_with_duplicates = orders_typed.unionByName(duplicates)

print("With duplicates:", orders_with_duplicates.count())
orders_with_duplicates \
    .groupBy("order_id") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()
# %%
dedup_window = (
    Window
    .partitionBy("order_id")
    .orderBy(F.col("ordered_at").desc())
)

stg_orders = (
    orders_with_duplicates
    .withColumn("row_number", F.row_number().over(dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

print("After deduplication:", stg_orders.count())

stg_orders \
    .groupBy("order_id") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()
# %%
# Reprise : l'ecriture de stg_orders n'avait pas ete executee (meme cellule que users, interrompue par l'erreur)
stg_orders.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
sh(f"aws s3 --profile default ls s3://{BUCKET}/silver/spark/ --recursive")
orders_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
assert orders_silver.count() == stg_orders.count()
print("asserts OK", orders_silver.count())
# %%
spark.stop()
