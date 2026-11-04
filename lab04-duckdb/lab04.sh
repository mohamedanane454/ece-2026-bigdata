# Lab 04 - SQL analytics avec DuckDB
cd /home/onyxia/work/$GIT_REPO_NAME
echo "$S3_ENDPOINT_URL $LAB_BUCKET_NAME"
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
command -v duckdb
duckdb --version
duckdb -c "SELECT 'Hello DuckDB' AS greeting, 21 * 2 AS answer;"
duckdb -c "SELECT name, type, provider, persistent, storage FROM duckdb_secrets();"
duckdb -c "SELECT name, regexp_replace(secret_string, '(key_id=)[^;]+', '\1***') AS secret_string FROM duckdb_secrets();" | grep -oE 'endpoint=[^;]*|url_style=[^;]*' | sort -u
cat > init.sql <<SQL
SET VARIABLE bucket = 's3://$LAB_BUCKET_NAME';
SET TimeZone = 'UTC';
SQL
cat init.sql
printf '*.duckdb\ninit.sql\n' >> .gitignore
cat > lab04_part1.sql <<'SQL'
SELECT getvariable('bucket') AS bucket;
-- Bronze
SELECT uuid, username, name, birthdate FROM read_csv(getvariable('bucket') || '/bronze/users.csv', header = true, strict_mode = false) LIMIT 3;
SELECT Delimiter, Quote, HasHeader FROM sniff_csv(getvariable('bucket') || '/bronze/users.csv');
DESCRIBE FROM read_csv(getvariable('bucket') || '/bronze/orders.csv');
SELECT column_name, column_type, approx_unique, null_percentage FROM (SUMMARIZE FROM read_csv(getvariable('bucket') || '/bronze/orders.csv'));
-- Chargement
CREATE OR REPLACE TABLE users AS FROM read_csv(getvariable('bucket') || '/bronze/users.csv');
CREATE OR REPLACE TABLE orders AS FROM read_csv(getvariable('bucket') || '/bronze/orders.csv');
.tables
SELECT (SELECT count(*) FROM users) AS users, (SELECT count(*) FROM orders) AS orders;
SELECT count(DISTINCT uuid) AS distinct_orders, count(DISTINCT user_uuid) AS distinct_users FROM orders;
SELECT min(date) AS first_order, max(date) AS last_order FROM orders;
-- Qualite
SELECT count(*) AS orphan_orders FROM orders o ANTI JOIN users u ON o.user_uuid = u.uuid;
SELECT count(*) AS inactive_users FROM users u ANTI JOIN orders o ON o.user_uuid = u.uuid;
SELECT uuid, count(*) AS occurrences FROM orders GROUP BY uuid HAVING count(*) > 1;
-- Agregations
SELECT product, count(*) AS orders, sum(quantity) AS quantity, round(100 * sum(quantity) / sum(sum(quantity)) OVER (), 1) AS share_pct FROM orders GROUP BY product ORDER BY quantity DESC;
SELECT date_trunc('month', date) AS month, count(*) AS orders, sum(quantity) AS quantity FROM orders GROUP BY month ORDER BY month;
-- Jointures
SELECT u.username, u.name, count(*) AS orders, sum(o.quantity) AS quantity FROM orders o JOIN users u ON o.user_uuid = u.uuid GROUP BY ALL ORDER BY quantity DESC LIMIT 5;
SELECT (date_diff('year', u.birthdate, DATE '2020-01-01') // 20) * 20 AS age_group, count(DISTINCT u.uuid) AS users, count(*) AS orders, round(avg(o.quantity), 2) AS avg_quantity FROM orders o JOIN users u ON o.user_uuid = u.uuid GROUP BY age_group ORDER BY age_group;
-- Fenetres
WITH daily AS (SELECT date::DATE AS day, sum(quantity) AS quantity FROM orders GROUP BY day)
SELECT day, quantity, sum(quantity) OVER (ORDER BY day) AS cumulative, round(avg(quantity) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS avg_7d FROM daily ORDER BY day LIMIT 10;
SELECT strftime(date, '%Y-%m') AS month, product, sum(quantity) AS quantity FROM orders GROUP BY month, product QUALIFY rank() OVER (PARTITION BY month ORDER BY sum(quantity) DESC) = 1 ORDER BY month;
-- Pivot
PIVOT (SELECT strftime(date, '%Y-%m') AS month, product, quantity FROM orders) ON product USING sum(quantity) ORDER BY month;
-- ===== Exercices =====
-- 1. Nombre moyen de commandes par utilisateur et quantite moyenne par commande
SELECT round(count(*) / count(DISTINCT user_uuid), 2) AS avg_orders_per_user, round(avg(quantity), 2) AS avg_quantity_per_order FROM orders;
-- 2. Premiere / derniere commande et nombre de jours entre les deux, par utilisateur
SELECT u.username, min(o.date) AS first_order, max(o.date) AS last_order, date_diff('day', min(o.date), max(o.date)) AS days_between FROM orders o JOIN users u ON o.user_uuid = u.uuid GROUP BY u.username ORDER BY first_order LIMIT 10;
-- 3. Heure de la journee avec la plus grande quantite vendue
SELECT hour(date) AS hour_of_day, sum(quantity) AS quantity FROM orders GROUP BY hour_of_day ORDER BY quantity DESC LIMIT 3;
-- 4. Variation mensuelle (%) de la quantite par produit, avec lag
WITH monthly AS (SELECT product, strftime(date, '%Y-%m') AS month, sum(quantity) AS quantity FROM orders GROUP BY ALL)
SELECT product, month, quantity, lag(quantity) OVER (PARTITION BY product ORDER BY month) AS previous, round(100.0 * (quantity - lag(quantity) OVER (PARTITION BY product ORDER BY month)) / lag(quantity) OVER (PARTITION BY product ORDER BY month), 1) AS variation_pct FROM monthly ORDER BY product, month;
-- 5. Utilisateurs ayant commande chaque produit au moins une fois
SELECT u.username, count(DISTINCT o.product) AS products FROM orders o JOIN users u ON o.user_uuid = u.uuid GROUP BY u.username HAVING count(DISTINCT o.product) = (SELECT count(DISTINCT product) FROM orders) ORDER BY u.username;
SELECT count(*) AS users_with_all_products FROM (SELECT user_uuid FROM orders GROUP BY user_uuid HAVING count(DISTINCT product) = 6);
-- Parquet
COPY orders TO (getvariable('bucket') || '/analytics/orders.parquet') (FORMAT parquet);
SELECT path_in_schema, type, compression, total_compressed_size, total_uncompressed_size FROM parquet_metadata(getvariable('bucket') || '/analytics/orders.parquet');
COPY orders TO (getvariable('bucket') || '/analytics/orders_by_product') (FORMAT parquet, PARTITION_BY (product));
SELECT file FROM glob(getvariable('bucket') || '/analytics/orders_by_product/**');
EXPLAIN ANALYZE SELECT count(*) FROM read_parquet(getvariable('bucket') || '/analytics/orders_by_product/*/*.parquet') WHERE product = 'cookie';
SQL
duckdb -echo analytics.duckdb -init init.sql < lab04_part1.sql
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/analytics/" --recursive
# --- CSV vs Parquet a grande echelle
uv run dataset-orders -u 5000 -o csv > orders_large.csv
ls -lh orders_large.csv
s5cmd --profile 'default' cp orders_large.csv "s3://$LAB_BUCKET_NAME/large/orders.csv"
cat > lab04_part2.sql <<'SQL'
.timer on
COPY (FROM read_csv(getvariable('bucket') || '/large/orders.csv', strict_mode=false)) TO (getvariable('bucket') || '/large/orders.parquet') (FORMAT parquet);
SELECT count(*) FROM read_parquet(getvariable('bucket') || '/large/orders.parquet');
SQL
duckdb -echo analytics.duckdb -init init.sql < lab04_part2.sql
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/large/"
for query in \
  "FROM read_csv(getvariable('bucket') || '/large/orders.csv', strict_mode=false) SELECT product, sum(quantity) GROUP BY product" \
  "FROM read_parquet(getvariable('bucket') || '/large/orders.parquet') SELECT product, sum(quantity) GROUP BY product" \
  "FROM read_parquet(getvariable('bucket') || '/large/orders.parquet') SELECT count(*) WHERE date >= '2100-01-01'"
do
  echo "$query"
  duckdb -init init.sql -c "SET enable_external_file_cache = false; EXPLAIN ANALYZE $query" 2>/dev/null | grep -E 'in:|#GET|Total Time'
done
duckdb -init init.sql -c "SELECT row_group_id, row_group_num_rows, stats_min, stats_max FROM parquet_metadata(getvariable('bucket') || '/large/orders.parquet') WHERE path_in_schema = 'date'"
duckdb -init init.sql -c "SELECT count(*) AS row_groups, count(*) FILTER (WHERE stats_max::TIMESTAMPTZ < TIMESTAMPTZ '2100-01-01') AS skipped FROM parquet_metadata(getvariable('bucket') || '/large/orders.parquet') WHERE path_in_schema = 'date'"
# --- API Python
uv add duckdb
cat <<'PY' >src/$UV_PROJECT_NAME/orders_report.py
import argparse
import os

import duckdb


def orders_report(product=None):
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    bronze = f"s3://{os.environ['LAB_BUCKET_NAME']}/bronze"
    return con.execute(
        f"""
        SELECT strftime(date, '%Y-%m') AS month, count(*) AS orders, sum(quantity) AS quantity
        FROM read_csv('{bronze}/orders.csv', strict_mode = false)
        WHERE $product IS NULL OR product = $product
        GROUP BY month
        ORDER BY month
        """,
        {"product": product},
    ).fetchall()


def main():
    parser = argparse.ArgumentParser(prog="orders-report", description="Monthly orders report")
    parser.add_argument("-p", "--product", help="Filter the orders on a product.")
    args = parser.parse_args()
    for month, orders, quantity in orders_report(args.product):
        print(f"{month}\t{orders}\t{quantity}")


if __name__ == "__main__":
    main()
PY
grep -q '^orders-report' pyproject.toml || sed -i '/^dataset-orders = /a orders-report = "ece_2026_bigdata.orders_report:main"' pyproject.toml
sed -n '/project.scripts/,/^$/p' pyproject.toml
uv run orders-report
uv run orders-report -p cookie
rm -f lab04_part1.sql lab04_part2.sql
git add .gitignore pyproject.toml uv.lock src
git commit -m "feat: orders report with duckdb"
gpush
# --- Cleanup
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/analytics/" --recursive
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/large/" --recursive
rm -f orders_large.csv
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/" --recursive
true
