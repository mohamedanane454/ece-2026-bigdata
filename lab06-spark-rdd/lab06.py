# Lab 06 - Spark RDD API (fichiers locaux users.csv / orders.csv)
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import json, time, urllib.request
spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
def stages():
    # Equivalent de l'onglet Stages de la Spark UI (API REST sur le port 4040)
    app = sc.applicationId
    data = json.load(urllib.request.urlopen(f"http://localhost:4040/api/v1/applications/{app}/stages"))
    for s in sorted(data, key=lambda s: s["stageId"]):
        print(s["stageId"], s["status"], s["name"][:60], "tasks=", s["numTasks"], "shuffleRead=", s["shuffleReadBytes"], "shuffleWrite=", s["shuffleWriteBytes"])
sc.defaultParallelism
# %%
users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")
print(users_lines.getNumPartitions())
print(orders_lines.getNumPartitions())
# %%
orders_repartitioned = orders_lines.repartition(8)
print(orders_repartitioned.getNumPartitions())
print(orders_repartitioned.coalesce(2).getNumPartitions())
print(orders_repartitioned.coalesce(2).toDebugString().decode())
# %%
header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
print(orders_data.toDebugString().decode())
# %%
print(orders_data.count())
print(orders_data.count())
stages()
# %%
for line in users_lines.take(6):
    print(line)
print(users_lines.count())
# %%
import re

UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
print(users_records.count())
# %%
def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }

orders_rdd = orders_data.map(parse_order).cache()
orders_rdd.take(3)
# %%
from operator import add

user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
user_quantity.take(5)
# %%
user_quantity_grouped = (
    orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .groupByKey()
    .mapValues(sum)
)
user_quantity_grouped.take(5)
# %%
# Comparaison des shuffles reduceByKey / groupByKey (onglet Stages de la Spark UI)
stages()
# %%
def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b

orders_dedup = (
    orders_rdd
    .map(lambda o: (o["order_id"], o))
    .reduceByKey(most_recent)
    .values()
)

print(orders_rdd.count(), orders_dedup.count())
# %%
def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]  # uuid, username

users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))

enriched = orders_kv.join(users_kv)
enriched.take(3)
# %%
left = orders_kv.leftOuterJoin(users_kv)
print(left.count(), left.filter(lambda kv: kv[1][1] is None).count())
# %%
# Exercice 1 : aggregateByKey -> (nombre de commandes, quantite totale) puis moyenne
stats = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).aggregateByKey(
    (0, 0),
    lambda acc, q: (acc[0] + 1, acc[1] + q),
    lambda a, b: (a[0] + b[0], a[1] + b[1]),
)
avg_quantity = stats.mapValues(lambda c: round(c[1] / c[0], 2))
print(stats.take(3))
print(avg_quantity.take(3))
# %%
# Exercice 2 : un user_id inconnu injecte, puis leftOuterJoin
fake = sc.parallelize([{"order_id": "fake-order", "user_id": "00000000-0000-0000-0000-000000000000", "date": "2020-01-01", "quantity": 1, "product": "bread"}])
orders_with_fake = orders_rdd.union(fake)
unmatched = orders_with_fake.map(lambda o: (o["user_id"], o)).leftOuterJoin(users_kv).filter(lambda kv: kv[1][1] is None)
print("orders without user:", unmatched.count())
# %%
# Exercice 3 : la meme agregation avec l'API DataFrame
df = spark.read.csv("orders.csv", header=True, inferSchema=True)
df_quantity = df.groupBy("user_uuid").sum("quantity")
df_quantity.show(5, truncate=False)
df_quantity.explain()
print(user_quantity.toDebugString().decode())
# %%
# Exercice 4 : temps avec et sans cache
t = time.perf_counter(); orders_rdd.count(); user_quantity.count(); print("avec cache  : %.3f s" % (time.perf_counter() - t))
print("stockage:", [(r.id(), r.getStorageLevel()) for r in [orders_rdd]], sc._jsc.sc().getRDDStorageInfo().length)
orders_nocache = orders_data.map(parse_order)
uq_nocache = orders_nocache.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
t = time.perf_counter(); orders_nocache.count(); uq_nocache.count(); print("sans cache  : %.3f s" % (time.perf_counter() - t))
# %%
spark.stop()
