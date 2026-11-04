# Lab 06 - Introduction a l'API RDD de Spark
cd /home/onyxia/work/$GIT_REPO_NAME
aws s3 --profile 'default' cp "s3://$LAB_BUCKET_NAME/bronze/users.csv" users.csv
aws s3 --profile 'default' cp "s3://$LAB_BUCKET_NAME/bronze/orders.csv" orders.csv
ls -la users.csv orders.csv
# Les cellules du lab sont executees comme un notebook (pyspark --master local[*])
spark-submit --master 'local[*]' --conf spark.ui.showConsoleProgress=false /home/onyxia/work/labkit/cells.py /home/onyxia/work/labkit/lab06.py 2>/dev/null
rm -f users.csv orders.csv
true
