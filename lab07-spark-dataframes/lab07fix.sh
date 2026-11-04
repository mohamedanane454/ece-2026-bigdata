# Lab 07 - reprise de l'ecriture Parquet de users et de la comparaison avec dbt
cd /home/onyxia/work/$GIT_REPO_NAME
# Cause : des multipart uploads incomplets laisses par le pod tue (OOMKilled) sous silver/spark/ ; le committer S3A plante en les listant.
aws s3api --profile default list-multipart-uploads --bucket "$LAB_BUCKET_NAME" --prefix silver/ | jq -c '.Uploads[]? | {Key, UploadId, Initiated}'
aws s3api --profile default list-multipart-uploads --bucket "$LAB_BUCKET_NAME" --prefix silver/ | jq -r '.Uploads[]? | [.Key, .UploadId] | @tsv' | while IFS=$(printf "\t") read -r key id; do aws s3api --profile default abort-multipart-upload --bucket "$LAB_BUCKET_NAME" --key "$key" --upload-id "$id" && echo "aborted $key"; done
aws s3api --profile default list-multipart-uploads --bucket "$LAB_BUCKET_NAME" --prefix silver/
spark-submit --master 'local[*]' --driver-memory 1g --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 --conf spark.ui.showConsoleProgress=false /home/onyxia/work/labkit/cells.py /home/onyxia/work/labkit/lab07fix.py 2>&1 | grep --line-buffered -vE '^(Ivy|:: |\s+confs|\s+found|\s+org\.|\s+---|\s+\||downloading|\[SUCCESSFUL|resolving|loading settings|Picked up|WARNING: )'
duckdb -c "
  SELECT 'dbt' AS engine, count(*) AS users
  FROM 's3://$LAB_BUCKET_NAME/silver/dbt/users.parquet'
  UNION ALL
  SELECT 'spark', count(*)
  FROM 's3://$LAB_BUCKET_NAME/silver/spark/users.parquet/*.parquet'
"
duckdb -c "DESCRIBE SELECT * FROM 's3://$LAB_BUCKET_NAME/silver/dbt/users.parquet'"
duckdb -c "DESCRIBE SELECT * FROM 's3://$LAB_BUCKET_NAME/silver/spark/users.parquet/*.parquet'"
true
