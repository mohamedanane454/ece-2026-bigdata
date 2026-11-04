# Lab 01 - Essential Linux Commands (dans ~/data-platform-lab)
mkdir -p ~/data-platform-lab
cd ~/data-platform-lab
mkdir bronze
mkdir -p data/silver/customers
mkdir -p data/gold/metrics
# correction de l'enonce : data/bronze n'est pas cree mais il est utilise ensuite
mkdir -p data/bronze
ls -lR .
ls -lha .
cat > data/bronze/customers.csv << 'EOF'
id,name,email,created_at
1,Alice Johnson,alice@example.com,2024-01-15
2,Bob Smith,bob@example.com,2024-02-20
3,Carol White,carol@example.com,2024-03-10
EOF
cat data/bronze/customers.csv
cat > data/bronze/spark_log.txt << 'EOF'
2024-01-15T10:23:45 INFO Starting Spark job
2024-01-15T10:23:46 INFO Initializing RDD
2024-01-15T10:23:47 DEBUG Partition 1 assigned to worker-1
2024-01-15T10:23:48 DEBUG Partition 2 assigned to worker-2
2024-01-15T10:24:00 INFO Map phase complete
2024-01-15T10:24:15 INFO Shuffle phase complete
2024-01-15T10:24:30 INFO Reduce phase complete
2024-01-15T10:24:45 INFO Job succeeded
2024-01-15T10:24:46 INFO Output written to HDFS
2024-01-15T10:24:47 INFO Cleaning up
EOF
head -n 5 data/bronze/spark_log.txt
head data/bronze/spark_log.txt
tail -n 3 data/bronze/spark_log.txt
# tail -f : terminal 1 simule en arriere-plan pendant 5 s, terminal 2 ajoute des lignes
timeout 5 tail -f data/bronze/spark_log.txt > /tmp/tailf.out &
sleep 2 && echo "2024-01-15T10:25:00 INFO New event" >> data/bronze/spark_log.txt
sleep 1 && echo "2024-01-15T10:25:01 INFO Another event" >> data/bronze/spark_log.txt
wait
cat /tmp/tailf.out
cp data/bronze/customers.csv data/bronze/customers_archive.csv
ls -l data/bronze/
cp -r data/bronze data/bronze_backup
ls -lR data/bronze_backup/
cp -v data/bronze/spark_log.txt data/silver/spark_log.txt
mv data/silver/spark_log.txt data/silver/spark_log_archive.txt
ls -l data/silver/
mv data/bronze/customers_archive.csv data/silver/
ls -l data/silver/
grep -c "ERROR" data/bronze/spark_log.txt
grep -in "info" data/bronze/spark_log.txt
grep -B1 -A1 "phase complete" data/bronze/spark_log.txt
# Bonus (les commandes kubectl logs sont faites dans le lab 02)
find data/ -type d | while read dir; do echo "$dir: $(ls $dir | wc -l)"; done
true
