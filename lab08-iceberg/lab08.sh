# Lab 08 - Explorer les tables Iceberg avec Spark
cd /home/onyxia/work/$GIT_REPO_NAME
echo "$LAB_BUCKET_NAME"
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/warehouse/" --recursive
uv run pyspark --version
spark-submit --master 'local[*]' --driver-memory 1g --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 --conf spark.ui.showConsoleProgress=false /home/onyxia/work/labkit/cells.py /home/onyxia/work/labkit/lab08.py 2>&1 | grep --line-buffered -vE '^(Ivy|:: |\s+confs|\s+found|\s+org\.|\s+---|\s+\||downloading|\[SUCCESSFUL|resolving|loading settings|Picked up|WARNING: )'
# Cleanup (suppression du warehouse) volontairement NON execute : le lab 09 enregistre ces tables dans Polaris
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/warehouse/silver/" --recursive | awk '{print $4}' | sed 's#/[^/]*$##' | sort | uniq -c
true
