# Lab 09 - detail des erreurs de l'analyst et etat final du catalogue
cd /home/onyxia/work/$GIT_REPO_NAME
export POLARIS_URI="http://polaris:8181"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
export AWS_REGION="us-east-1"
set +x
set -a; . /home/onyxia/work/polaris-lab/.polaris-credentials.env; set +a
set -x
spark-submit --master 'local[*]' --driver-memory 1g --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 --conf spark.ui.showConsoleProgress=false /home/onyxia/work/labkit/cells.py /home/onyxia/work/labkit/lab09d.py 2>&1 | grep --line-buffered -vE '^(Ivy|:: |\s+confs|\s+found|\s+org\.|\s+---|\s+\||downloading|\[SUCCESSFUL|resolving|loading settings|Picked up|WARNING: )'
true
