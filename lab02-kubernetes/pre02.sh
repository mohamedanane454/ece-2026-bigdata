kubectl auth can-i create namespaces
kubectl auth can-i create deployments
kubectl auth can-i create services
kubectl auth can-i create cronjobs
kubectl auth can-i get nodes
kubectl describe quota
kubectl describe limitrange
kubectl get nodes 2>&1 | head -5
kubectl top pods 2>&1 | head -5
kubectl get sts -o wide
kubectl get sts -o jsonpath='{.items[0].spec.template.spec.containers[0].resources}'; echo
which jq helm uv duckdb pyspark spark-submit
python3 --version; java -version 2>&1 | head -1
echo SPARK_HOME=$SPARK_HOME; ls $SPARK_HOME/jars | grep -iE 'hadoop-aws|spark-core|iceberg'
nproc; free -m
