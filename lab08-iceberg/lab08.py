import os, subprocess
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
def sh(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print("$ " + cmd + "\n" + r.stdout + r.stderr)

os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
spark.sql("SHOW NAMESPACES IN lab").show()
# %%
# La table lab.silver.orders existe deja (lab 07) : pas besoin de la recreer
spark.sql("SHOW TABLES IN lab.silver").show()
spark.sql("""
    SELECT count(*) AS orders,
           sum(quantity) AS quantity
    FROM lab.silver.orders
""").show()
# %%
sh(f"aws s3 --profile default ls s3://{BUCKET}/warehouse/silver/orders/ --recursive")
# %%
spark.sql("SELECT * FROM lab.silver.orders.snapshots").show(truncate=False)
spark.sql("""
    SELECT file_path, file_format, record_count, file_size_in_bytes
    FROM lab.silver.orders.files
""").show(truncate=False)
# %%
stg_orders = spark.table("lab.silver.orders")
stg_orders.show(5)
new_orders = stg_orders.limit(3).withColumn("order_id", F.expr("uuid()"))
new_orders.writeTo("lab.silver.orders").append()
spark.sql("""
    SELECT snapshot_id, operation, committed_at
    FROM lab.silver.orders.snapshots
    ORDER BY committed_at
""").show(truncate=False)
spark.sql("SELECT count(*) FROM lab.silver.orders").show()
spark.sql("SELECT count(*) AS data_files FROM lab.silver.orders.files").show()
# %%
snapshots = spark.sql("""
    SELECT snapshot_id
    FROM lab.silver.orders.snapshots
    ORDER BY committed_at
""").collect()
snapshots
# %%
first_snapshot = snapshots[0]["snapshot_id"]
spark.sql(f"""
    SELECT count(*)
    FROM lab.silver.orders
    VERSION AS OF {first_snapshot}
""").show()
spark.sql("SELECT count(*) FROM lab.silver.orders").show()
# %%
spark.sql("ALTER TABLE lab.silver.orders ADD COLUMN source STRING")
spark.sql("DESCRIBE lab.silver.orders").show()
spark.sql("""
    INSERT INTO lab.silver.orders
    SELECT order_id, user_id, ordered_at, quantity, product, 'spark'
    FROM lab.silver.orders
    LIMIT 1
""")
spark.sql("SELECT source, count(*) FROM lab.silver.orders GROUP BY source").show()
sh(f"aws s3 --profile default ls s3://{BUCKET}/warehouse/silver/orders/ --recursive")
# %%
# Comparaison Iceberg / Parquet
sh(f"aws s3 --profile default ls s3://{BUCKET}/silver/spark/orders.parquet/ --recursive")
# %%
# Exercices 1 a 4 sur lab.silver.users
spark.sql("SHOW TABLES IN lab.silver").show()
users = spark.table("lab.silver.users")
print("users:", users.count())
batch = users.limit(2).withColumn("user_id", F.expr("uuid()")).withColumn("username", F.concat(F.col("username"), F.lit("_new")))
batch.writeTo("lab.silver.users").append()
snaps = spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.users.snapshots ORDER BY committed_at")
snaps.show(truncate=False)
s = snaps.collect()
spark.sql(f"SELECT count(*) AS before_append FROM lab.silver.users VERSION AS OF {s[0]['snapshot_id']}").show()
spark.sql(f"SELECT count(*) AS after_append FROM lab.silver.users VERSION AS OF {s[-1]['snapshot_id']}").show()
spark.sql("ALTER TABLE lab.silver.users ADD COLUMN country STRING")
spark.table("lab.silver.users").limit(1).withColumn("user_id", F.expr("uuid()")).withColumn("country", F.lit("US")).writeTo("lab.silver.users").append()
spark.sql("SELECT country, count(*) FROM lab.silver.users GROUP BY country").show()
# %%
# Exercice 5 : layout S3 des deux tables
sh(f"aws s3 --profile default ls s3://{BUCKET}/warehouse/silver/ --recursive")
# %%
# Retour au schema initial de users (le lab 09 ajoute lui-meme une colonne country)
spark.sql("ALTER TABLE lab.silver.users DROP COLUMN country")
spark.sql("DESCRIBE lab.silver.users").show()
# %%
spark.stop()
