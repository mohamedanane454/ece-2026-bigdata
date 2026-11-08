import os, subprocess, json, urllib.request, urllib.parse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
def sh(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print("$ " + cmd + "\n" + r.stdout + r.stderr)
def err(e):
    m = str(e).strip().splitlines()
    print("ERREUR (attendue) :", type(e).__name__, "-", m[0][:300] if m else "")
BUCKET = os.environ["LAB_BUCKET_NAME"]
POLARIS_URI = os.environ["POLARIS_URI"]
POLARIS_CATALOG = os.environ["POLARIS_CATALOG"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
def make_session(principal, credential=None):
    credential = credential or os.environ[f"POLARIS_{principal.upper()}_CREDENTIAL"]
    s = (
        SparkSession.builder
        .appName("lab-polaris")
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
        .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION"))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lab.type", "hadoop")
        .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
        .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type", "rest")
        .config("spark.sql.catalog.polaris.uri", f"{POLARIS_URI}/api/catalog")
        .config("spark.sql.catalog.polaris.warehouse", POLARIS_CATALOG)
        .config("spark.sql.catalog.polaris.credential", credential)
        .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    print("Polaris principal:", principal)
    return s
spark = make_session("dev")
# Le cleanup du lab 09 a supprime polaris.silver.users, necessaire au lab Trino : on la recree depuis la couche silver Spark (50 users)
spark.sql("SHOW TABLES IN polaris.silver").show()
users = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
print("users silver/spark :", users.count())
users.writeTo("polaris.silver.users").createOrReplace()
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.sql("SELECT count(*) AS users FROM polaris.silver.users").show()
spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()
# %%
# Lab 10 - exercice 5 (partie Spark) : meme agregation que gold.orders_by_state_month, depuis Spark connecte a Polaris
agg = spark.sql("""
    SELECT u.state, date_trunc('month', o.ordered_at) AS month,
           count(*) AS orders, sum(o.quantity) AS units, count(DISTINCT o.user_id) AS buyers
    FROM polaris.silver.orders o
    JOIN polaris.silver.users u ON u.user_id = o.user_id
    GROUP BY 1, 2
""")
n = agg.count()
tot = agg.agg(F.sum("orders").alias("orders"), F.sum("units").alias("units")).collect()[0]
print("SPARK orders_by_state_month rows =", n, "orders =", tot["orders"], "units =", tot["units"])
open("/home/onyxia/work/labkit/lab10_spark_result.txt", "w").write(f"{n} {tot['orders']} {tot['units']}\n")
agg.orderBy("month", "state").show(5)
spark.stop()
