import os, subprocess, json, urllib.request, urllib.parse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
def sh(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print("$ " + cmd + "\n" + r.stdout + r.stderr)
def err(e):
    m = str(e).strip().splitlines()
    print("ERREUR (attendue) :", type(e).__name__, "-", m[0][:300] if m else "")
    for l in m:
        if "Exception" in l or "Forbidden" in l or "authorized" in l:
            print("   ", l.strip()[:400])
            break
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
# Exercice 1 : suppression de la colonne country
spark.sql("SELECT snapshot_id, operation FROM polaris.silver.users.snapshots").show(truncate=False)
spark.sql("ALTER TABLE polaris.silver.users DROP COLUMN country")
spark.sql("DESCRIBE polaris.silver.users").show()
spark.sql("SELECT snapshot_id, operation FROM polaris.silver.users.snapshots").show(truncate=False)
spark.sql("SELECT timestamp, file FROM polaris.silver.users.metadata_log_entries").show(truncate=False)
# %%
# Exercice 2 : role silver_reader limite au namespace silver
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")
spark.sql("CREATE OR REPLACE TABLE polaris.gold.sales_by_product AS SELECT product, sum(quantity) AS quantity FROM polaris.silver.orders GROUP BY product")
spark.sql("SELECT * FROM polaris.gold.sales_by_product").show()
API = POLARIS_URI + "/api/management/v1"
tok = json.load(urllib.request.urlopen(POLARIS_URI + "/api/catalog/v1/oauth/tokens", data=urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": "root", "client_secret": "s3cr3t", "scope": "PRINCIPAL_ROLE:ALL"}).encode()))["access_token"]
def call(method, path, body=None):
    req = urllib.request.Request(API + path, method=method, data=json.dumps(body).encode() if body is not None else None, headers={"Authorization": "Bearer " + tok, "Content-Type": "application/json"})
    try:
        r = urllib.request.urlopen(req)
        txt = r.read().decode()
        print(method, path, "->", r.status)
        return json.loads(txt) if txt else None
    except urllib.error.HTTPError as e:
        print(method, path, "->", e.code, e.read().decode()[:200])
        return None
cat = POLARIS_CATALOG
call("POST", f"/catalogs/{cat}/catalog-roles", {"catalogRole": {"name": "silver_reader"}})
call("PUT", f"/catalogs/{cat}/catalog-roles/silver_reader/grants", {"grant": {"type": "catalog", "privilege": "CATALOG_READ_PROPERTIES"}})
for p in ["NAMESPACE_READ_PROPERTIES", "TABLE_LIST", "TABLE_READ_PROPERTIES", "TABLE_READ_DATA"]:
    call("PUT", f"/catalogs/{cat}/catalog-roles/silver_reader/grants", {"grant": {"type": "namespace", "namespace": ["silver"], "privilege": p}})
print(json.dumps(call("GET", f"/catalogs/{cat}/catalog-roles/silver_reader/grants")))
created = call("POST", "/principals", {"principal": {"name": "silver_reader_user"}})
if created is None:
    created = call("POST", "/principals/silver_reader_user/rotate")
reader_cred = created["credentials"]["clientId"] + ":" + created["credentials"]["clientSecret"]
call("POST", "/principal-roles", {"principalRole": {"name": "silver_consumer"}})
call("PUT", "/principals/silver_reader_user/principal-roles", {"principalRole": {"name": "silver_consumer"}})
call("PUT", f"/principal-roles/silver_consumer/catalog-roles/{cat}", {"catalogRole": {"name": "silver_reader"}})
# %%
spark.stop()
spark = make_session("silver_reader_user", reader_cred)
spark.sql("SELECT count(*) AS silver_orders FROM polaris.silver.orders").show()
try:
    spark.sql("SELECT * FROM polaris.gold.sales_by_product").show()
except Exception as e:
    err(e)
# %%
# Cleanup en tant que dev
spark.stop()
spark = make_session("dev")
spark.sql("DROP TABLE IF EXISTS polaris.gold.sales_by_product")
spark.sql("DROP TABLE IF EXISTS polaris.silver.users")
spark.sql("DROP NAMESPACE IF EXISTS polaris.gold")
spark.sql("SHOW NAMESPACES IN polaris").show()
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.stop()
