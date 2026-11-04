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
print(spark.sparkContext.getConf().get("spark.hadoop.fs.s3.impl", None))
spark.sql("SHOW NAMESPACES IN lab").show()
spark.sql("SHOW NAMESPACES IN polaris").show()
# %%
# Enregistrement de lab.silver.orders dans Polaris (sans copie des donnees)
sh(f"aws s3 --profile default ls s3://{BUCKET}/warehouse/silver/orders/ --recursive | wc -l")
sh(f"aws s3 --profile default cp s3://{BUCKET}/warehouse/silver/orders/metadata/version-hint.text -")
sh(f"aws s3 --profile default ls s3://{BUCKET}/warehouse/silver/orders/metadata/ | grep metadata.json")
version = subprocess.run(f"aws s3 --profile default cp s3://{BUCKET}/warehouse/silver/orders/metadata/version-hint.text -", shell=True, capture_output=True, text=True).stdout.strip()
print("version courante :", version)
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
spark.sql(f"""
    CALL polaris.system.register_table(
      table => 'silver.orders',
      metadata_file => 's3a://{BUCKET}/warehouse/silver/orders/metadata/v{version}.metadata.json'
    )
""").show()
spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()
spark.sql("SELECT count(*) AS orders FROM lab.silver.orders").show()
sh(f"aws s3 --profile default ls s3://{BUCKET}/warehouse/silver/orders/ --recursive | wc -l")
# %%
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots").show(truncate=False)
# %%
stg_users = spark.table("lab.silver.users")
stg_users.writeTo("polaris.silver.users").createOrReplace()
spark.sql("SELECT count(*) AS users FROM polaris.silver.users").show()
sh(f"aws s3 --profile default ls s3://{BUCKET}/warehouse/silver/ --recursive | awk '{{print $4}}'")
# %%
spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN country STRING")
spark.sql("DESCRIBE polaris.silver.users").show()
spark.sql("SELECT timestamp, file FROM polaris.silver.users.metadata_log_entries").show(truncate=False)
# %%
# Lecture en tant qu'analyst (nouvelle session Spark = redemarrage du kernel)
spark.stop()
spark = make_session("analyst")
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.sql("SELECT product, count(*) FROM polaris.silver.orders GROUP BY product").show()
# %%
try:
    spark.sql("INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
except Exception as e:
    err(e)
# %%
try:
    spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN phone STRING")
except Exception as e:
    err(e)
# %%
# Retour a dev : le INSERT refuse a-t-il cree un snapshot ?
spark.stop()
spark = make_session("dev")
spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots ORDER BY committed_at").show(truncate=False)
spark.stop()
