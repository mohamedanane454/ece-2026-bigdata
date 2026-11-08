import os
import pandas as pd
import trino
pd.set_option("display.width", 250); pd.set_option("display.max_columns", 20); pd.set_option("display.max_colwidth", 90)

CATALOG = "lakehouse"

conn = trino.dbapi.connect(
    host=os.environ["TRINO_HOST"],
    port=int(os.environ["TRINO_PORT"]),
    user=os.environ["TRINO_USER"],
    http_scheme="http",
    catalog=CATALOG,
)

def sql(statement):
    cur = conn.cursor()
    cur.execute(statement)
    rows = cur.fetchall()  # the query only completes when its results are fetched
    columns = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)

def input_lines(plan_text):
    return [l.strip() for l in plan_text.splitlines() if "Input:" in l or "ScanFilter" in l or "TableScan" in l][:8]
sql("SELECT version()")
# %%
sql("SHOW CATALOGS")
# %%
print(sql("SHOW SCHEMAS FROM lakehouse"))
print(sql("SHOW TABLES FROM lakehouse.silver"))
# %%
print(sql("DESCRIBE lakehouse.silver.orders"))
print(sql("DESCRIBE lakehouse.silver.users"))
# %%
print(sql("SELECT count(*) AS orders FROM lakehouse.silver.orders"))
print(sql("SELECT count(*) AS users FROM lakehouse.silver.users"))
# %%
sql("""
    SELECT state, count(*) AS users, count_if(city IS NULL) AS missing_city
    FROM lakehouse.silver.users
    GROUP BY state
    ORDER BY users DESC
    LIMIT 10
""")
# %%
sql("""
    SELECT count(*) AS orphan_orders
    FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id
    WHERE u.user_id IS NULL
""")
# %%
print(sql("""SELECT snapshot_id, committed_at, operation FROM lakehouse.silver."orders$snapshots" ORDER BY committed_at"""))
print(sql("""SELECT file_path, file_format, record_count, file_size_in_bytes FROM lakehouse.silver."orders$files" """))
print(sql('SELECT * FROM lakehouse.silver."orders$history"'))
# %%
import subprocess
print(subprocess.run("aws s3 --profile default ls s3://$LAB_BUCKET_NAME/warehouse/silver/orders/data/", shell=True, capture_output=True, text=True).stdout)
# %%
plan = sql("""
    EXPLAIN (TYPE DISTRIBUTED)
    SELECT u.state, count(*) AS orders
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY u.state
""")
print(plan.iloc[0, 0])
# %%
stats = sql("""
    EXPLAIN ANALYZE
    SELECT u.state, count(*) AS orders
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY u.state
""")
print(stats.iloc[0, 0])
# %%
try:
    print(sql("""
        SELECT query_id, state, date_diff('millisecond', created, coalesce("end", current_timestamp)) AS elapsed_ms, substr(query, 1, 70) AS query
        FROM system.runtime.queries ORDER BY created DESC LIMIT 5
    """))
except Exception as e:
    print("system.runtime.queries refuse :", e)
# %%
sql("CREATE SCHEMA IF NOT EXISTS lakehouse.gold")
print(sql("SHOW SCHEMAS FROM lakehouse"))
# %%
print(sql("""
    CREATE TABLE lakehouse.gold.orders_by_state_month AS
    SELECT
        u.state,
        date_trunc('month', o.ordered_at) AS month,
        count(*)                          AS orders,
        sum(o.quantity)                   AS units,
        count(DISTINCT o.user_id)         AS buyers
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY 1, 2
"""))
sql("SELECT * FROM lakehouse.gold.orders_by_state_month ORDER BY month, state LIMIT 10")
# %%
print(sql("""
    CREATE TABLE lakehouse.gold.top_products AS
    SELECT product, count(*) AS orders, sum(quantity) AS units,
           rank() OVER (ORDER BY sum(quantity) DESC) AS units_rank
    FROM lakehouse.silver.orders
    GROUP BY product
"""))
sql("SELECT * FROM lakehouse.gold.top_products ORDER BY units_rank LIMIT 10")
# %%
print(sql("""
    CREATE TABLE lakehouse.gold.user_activity AS
    SELECT
        u.user_id,
        u.state,
        date_diff('year', u.birthdate, current_date) AS age,
        count(o.order_id)        AS orders,
        coalesce(sum(o.quantity), 0) AS units,
        min(o.ordered_at)        AS first_order_at,
        max(o.ordered_at)        AS last_order_at
    FROM lakehouse.silver.users AS u
    LEFT JOIN lakehouse.silver.orders AS o ON o.user_id = u.user_id
    GROUP BY u.user_id, u.state, u.birthdate
"""))
sql("SELECT * FROM lakehouse.gold.user_activity ORDER BY orders DESC LIMIT 10")
# %%
print(sql("SHOW TABLES FROM lakehouse.gold"))
print(sql("""SELECT file_path, record_count, file_size_in_bytes FROM lakehouse.gold."top_products$files" """))
# %%
silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
gold_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.orders_by_state_month").iloc[0, 0]
product_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.top_products").iloc[0, 0]
assert silver_units == gold_units
assert silver_units == product_units
silver_users = sql("SELECT count(*) AS v FROM lakehouse.silver.users").iloc[0, 0]
gold_users = sql("SELECT count(*) AS v FROM lakehouse.gold.user_activity").iloc[0, 0]
silver_orders = sql("SELECT count(*) AS v FROM lakehouse.silver.orders").iloc[0, 0]
gold_orders = sql("SELECT sum(orders) AS v FROM lakehouse.gold.user_activity").iloc[0, 0]
assert silver_users == gold_users
assert silver_orders == gold_orders
print("asserts OK", silver_units, silver_users, silver_orders)
# %%
sql("DELETE FROM lakehouse.gold.top_products")
sql("""
    INSERT INTO lakehouse.gold.top_products
    SELECT product, count(*), sum(quantity), rank() OVER (ORDER BY sum(quantity) DESC)
    FROM lakehouse.silver.orders
    GROUP BY product
""")
sql("""SELECT snapshot_id, committed_at, operation FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at""")
# %%
first = sql("""SELECT snapshot_id FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at LIMIT 1""").iloc[0, 0]
print(sql(f"SELECT count(*) AS products FROM lakehouse.gold.top_products FOR VERSION AS OF {first}"))
print(sql("SELECT count(*) AS products FROM lakehouse.gold.top_products"))
# %%
# ===== Exercice 1 : table partitionnee par mois
print(sql("""
    CREATE TABLE lakehouse.gold.orders_by_state_month_part
    WITH (partitioning = ARRAY['month(month)']) AS
    SELECT * FROM lakehouse.gold.orders_by_state_month
"""))
print(sql('SELECT partition, record_count, file_size_in_bytes FROM lakehouse.gold."orders_by_state_month_part$files"'))
print(sql('SELECT record_count, file_size_in_bytes FROM lakehouse.gold."orders_by_state_month$files"'))
for t in ["orders_by_state_month", "orders_by_state_month_part"]:
    p = sql(f"EXPLAIN ANALYZE SELECT sum(units) FROM lakehouse.gold.{t} WHERE month = TIMESTAMP '2020-02-01 00:00:00 UTC'").iloc[0, 0]
    print(t, input_lines(p))
# %%
# ===== Exercice 2 : cohortes par decennie de naissance
print(sql("""
    CREATE TABLE lakehouse.gold.cohorts AS
    SELECT
        (year(u.birthdate) / 10) * 10             AS birth_decade,
        count(DISTINCT u.user_id)                 AS users,
        count(o.order_id)                         AS orders,
        round(avg(CAST(o.quantity AS double)), 2) AS avg_units_per_order
    FROM lakehouse.silver.users AS u
    LEFT JOIN lakehouse.silver.orders AS o ON o.user_id = u.user_id
    GROUP BY 1
"""))
sql("SELECT * FROM lakehouse.gold.cohorts ORDER BY birth_decade")
# %%
# ===== Exercice 5 : comparaison avec la meme agregation faite par Spark connecte a Polaris
spark_rows, spark_orders, spark_units = open("/home/onyxia/work/labkit/lab10_spark_result.txt").read().split()
trino = sql("SELECT count(*) AS n, sum(orders) AS orders, sum(units) AS units FROM lakehouse.gold.orders_by_state_month").iloc[0]
print("Spark :", spark_rows, spark_orders, spark_units, "| Trino :", trino["n"], trino["orders"], trino["units"])
# %%
# ===== Exercice 4 : OPTIMIZE
print(sql('SELECT snapshot_id, operation FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at'))
print(sql('SELECT file_path, record_count FROM lakehouse.gold."top_products$files"'))
sql("ALTER TABLE lakehouse.gold.top_products EXECUTE optimize")
print(sql('SELECT snapshot_id, operation FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at'))
print(sql('SELECT file_path, record_count FROM lakehouse.gold."top_products$files"'))
# %%
# ===== Exercice 3 : une commande de plus dans silver, MERGE du gold, time travel
gold_first = sql('SELECT snapshot_id FROM lakehouse.gold."orders_by_state_month$snapshots" ORDER BY committed_at DESC LIMIT 1').iloc[0, 0]
sql("""
    INSERT INTO lakehouse.silver.orders (order_id, user_id, ordered_at, quantity, product)
    SELECT CAST(uuid() AS varchar), user_id, ordered_at, 5, product
    FROM lakehouse.silver.orders
    WHERE ordered_at = (SELECT min(ordered_at) FROM lakehouse.silver.orders)
""")
print(sql("SELECT count(*) AS orders FROM lakehouse.silver.orders"))
sql("""
    MERGE INTO lakehouse.gold.orders_by_state_month AS t
    USING (
        SELECT u.state, date_trunc('month', o.ordered_at) AS month,
               count(*) AS orders, sum(o.quantity) AS units, count(DISTINCT o.user_id) AS buyers
        FROM lakehouse.silver.orders AS o
        JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
        GROUP BY 1, 2
    ) AS s
    ON t.state = s.state AND t.month = s.month
    WHEN MATCHED AND (t.orders <> s.orders OR t.units <> s.units OR t.buyers <> s.buyers)
        THEN UPDATE SET orders = s.orders, units = s.units, buyers = s.buyers
    WHEN NOT MATCHED THEN INSERT (state, month, orders, units, buyers) VALUES (s.state, s.month, s.orders, s.units, s.buyers)
""")
print(sql('SELECT snapshot_id, committed_at, operation FROM lakehouse.gold."orders_by_state_month$snapshots" ORDER BY committed_at'))
print(sql(f"SELECT sum(orders) AS orders, sum(units) AS units FROM lakehouse.gold.orders_by_state_month FOR VERSION AS OF {gold_first}"))
print(sql("SELECT sum(orders) AS orders, sum(units) AS units FROM lakehouse.gold.orders_by_state_month"))
