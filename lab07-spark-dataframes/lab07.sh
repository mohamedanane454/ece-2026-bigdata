# Lab 07 - Bronze -> Silver avec Spark DataFrames et Iceberg
# (2e execution : la 1re a ete interrompue par un OOMKilled du pod VS Code a 2000Mi ; limits passees a 2 CPU / 3000Mi)
cd /home/onyxia/work/$GIT_REPO_NAME
git log --oneline | head -3
echo "$LAB_BUCKET_NAME"
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
uv run pyspark --version
ls $SPARK_HOME/jars | grep -i hadoop-aws
spark-submit --master 'local[*]' --driver-memory 1g --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 --conf spark.ui.showConsoleProgress=false /home/onyxia/work/labkit/cells.py /home/onyxia/work/labkit/lab07.py 2>&1 | grep --line-buffered -vE '^(Ivy|:: |\s+confs|\s+found|\s+org\.|\s+---|\s+\||downloading|\[SUCCESSFUL|resolving|loading settings|Picked up|WARNING: )'
aws s3 --profile default sync "s3://$LAB_BUCKET_NAME/warehouse/" ./warehouse/
find warehouse -type f | sort
git add ./warehouse/silver/orders/metadata ./warehouse/silver/users/metadata
git commit -m "feat(spark-lab): add Iceberg table metadata"
gpush
duckdb -c "
  SELECT 'dbt' AS engine, count(*) AS orders, sum(quantity) AS quantity
  FROM 's3://$LAB_BUCKET_NAME/silver/dbt/orders.parquet'
  UNION ALL
  SELECT 'spark', count(*), sum(quantity)
  FROM 's3://$LAB_BUCKET_NAME/silver/spark/orders.parquet/*.parquet'
"
duckdb -c "
  SELECT 'dbt' AS engine, count(*) AS users
  FROM 's3://$LAB_BUCKET_NAME/silver/dbt/users.parquet'
  UNION ALL
  SELECT 'spark', count(*)
  FROM 's3://$LAB_BUCKET_NAME/silver/spark/users.parquet/*.parquet'
"
duckdb -c "DESCRIBE SELECT * FROM 's3://$LAB_BUCKET_NAME/silver/dbt/orders.parquet'"
duckdb -c "DESCRIBE SELECT * FROM 's3://$LAB_BUCKET_NAME/silver/spark/orders.parquet/*.parquet'"
# Cleanup volontairement NON execute : warehouse Iceberg et silver/spark reutilises par les labs 08 et 09
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/" --recursive | awk '{print $4}' | cut -d/ -f1-3 | sort | uniq -c
true
