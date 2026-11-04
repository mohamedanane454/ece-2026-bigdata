# Lab 09 / lab-2 - Gouvernance des tables Iceberg avec Polaris
cd /home/onyxia/work/$GIT_REPO_NAME
export POLARIS_URI="http://polaris:8181"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
export AWS_REGION="us-east-1"
echo "$LAB_BUCKET_NAME"
set +x
set -a; . /home/onyxia/work/polaris-lab/.polaris-credentials.env; set +a
echo "POLARIS_DEV_CREDENTIAL=${POLARIS_DEV_CREDENTIAL:0:4}... POLARIS_ANALYST_CREDENTIAL=${POLARIS_ANALYST_CREDENTIAL:0:4}..."
set -x
spark-submit --master 'local[*]' --driver-memory 1g --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 --conf spark.ui.showConsoleProgress=false /home/onyxia/work/labkit/cells.py /home/onyxia/work/labkit/lab09b.py 2>&1 | grep --line-buffered -vE '^(Ivy|:: |\s+confs|\s+found|\s+org\.|\s+---|\s+\||downloading|\[SUCCESSFUL|resolving|loading settings|Picked up|WARNING: )'
# Commit des metadonnees Iceberg (tables enregistrees / creees via Polaris)
aws s3 --profile default sync "s3://$LAB_BUCKET_NAME/warehouse/" ./warehouse/
git status --short | head -30
git add ./warehouse/silver/orders/metadata ./warehouse/silver/users/metadata
git commit -m "feat(spark-lab): add Polaris table metadata"
gpush
# Exercices et cleanup
spark-submit --master 'local[*]' --driver-memory 1g --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 --conf spark.ui.showConsoleProgress=false /home/onyxia/work/labkit/cells.py /home/onyxia/work/labkit/lab09x.py 2>&1 | grep --line-buffered -vE '^(Ivy|:: |\s+confs|\s+found|\s+org\.|\s+---|\s+\||downloading|\[SUCCESSFUL|resolving|loading settings|Picked up|WARNING: )'
true
