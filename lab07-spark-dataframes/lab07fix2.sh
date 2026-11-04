# Lab 07 - reprise de l'ecriture Parquet de orders et comparaison avec dbt
cd /home/onyxia/work/$GIT_REPO_NAME
spark-submit --master 'local[*]' --driver-memory 1g --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 --conf spark.ui.showConsoleProgress=false /home/onyxia/work/labkit/cells.py /home/onyxia/work/labkit/lab07fix2.py 2>&1 | grep --line-buffered -vE '^(Ivy|:: |\s+confs|\s+found|\s+org\.|\s+---|\s+\||downloading|\[SUCCESSFUL|resolving|loading settings|Picked up|WARNING: )'
duckdb -c "
  SELECT 'dbt' AS engine, count(*) AS orders, sum(quantity) AS quantity
  FROM 's3://$LAB_BUCKET_NAME/silver/dbt/orders.parquet'
  UNION ALL
  SELECT 'spark', count(*), sum(quantity)
  FROM 's3://$LAB_BUCKET_NAME/silver/spark/orders.parquet/*.parquet'
"
duckdb -c "DESCRIBE SELECT * FROM 's3://$LAB_BUCKET_NAME/silver/dbt/orders.parquet'"
duckdb -c "DESCRIBE SELECT * FROM 's3://$LAB_BUCKET_NAME/silver/spark/orders.parquet/*.parquet'"
true
