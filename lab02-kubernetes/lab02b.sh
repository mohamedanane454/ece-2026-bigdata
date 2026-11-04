# Lab 02 / lab-2 - ConfigMap & Secret
# Adaptation Onyxia : impossible de creer un namespace (droits limites), on utilise le namespace personnel.
kubectl create namespace lab-configmap-secret
NS=$KUBERNETES_NAMESPACE
mkdir -p ~/work/lab-configmap-secret && cd ~/work/lab-configmap-secret
# --- 2. ConfigMaps : methode 1 (fichier)
cat > pipeline-config.properties << 'EOF'
# Data Pipeline Configuration
bronze.path=/mnt/data/bronze
silver.path=/mnt/data/silver
gold.path=/mnt/data/gold
batch.size=1000
retention.days=90
log.level=INFO
spark.cores=4
kafka.brokers=kafka-0.kafka.svc.cluster.local:9092
EOF
kubectl -n $NS create configmap pipeline-config --from-file=pipeline-config.properties
kubectl -n $NS get configmap pipeline-config
kubectl -n $NS describe configmap pipeline-config
kubectl -n $NS get configmap pipeline-config -o yaml
# methode 2 (litteraux)
kubectl -n $NS create configmap data-paths --from-literal=bronze=/mnt/data/bronze --from-literal=silver=/mnt/data/silver --from-literal=gold=/mnt/data/gold
kubectl -n $NS get configmap data-paths -o yaml
# methode 3 (manifest YAML)
cat > configmap-data-platform.yaml << 'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: data-platform-config
data:
  log_level: INFO
  spark_workers: "4"
  batch_size: "1000"
  application.yaml: |
    # Data Platform Application Config
    pipeline:
      name: bronze-to-silver-etl
      schedule: "0 2 * * *"
      timeout_minutes: 120
    storage:
      type: s3
      endpoint: ceph-rgw.ceph-storage.svc.cluster.local:7480
      region: us-east-1
    kafka:
      bootstrap_servers: kafka-0.kafka.svc.cluster.local:9092
      consumer_group: data-platform-consumers
      topic_prefix: data.bronze
  table_mappings.csv: |
    source_table,bronze_path,silver_layer
    customers,/data/bronze/customers,deduplicated_customers
    orders,/data/bronze/orders,aggregated_orders
    products,/data/bronze/products,product_catalog
EOF
kubectl -n $NS apply -f configmap-data-platform.yaml
kubectl -n $NS get configmap data-platform-config -o yaml
kubectl -n $NS get configmap data-platform-config -o jsonpath='{.data.log_level}'; echo
kubectl -n $NS get configmap data-platform-config -o jsonpath='{.data.application\.yaml}'; echo
# --- 3. Secrets (valeurs d'exemple fournies par l'enonce, ce ne sont pas de vraies cles)
kubectl -n $NS create secret generic ceph-credentials --from-literal=access-key=<exemple-access-key> --from-literal=secret-key=<exemple-secret-key>
kubectl -n $NS get secret ceph-credentials -o yaml
cat > s3-credentials.txt << 'EOF'
[default]
aws_access_key_id = <exemple-access-key>
aws_secret_access_key = <exemple-secret-key>
EOF
kubectl -n $NS create secret generic s3-credentials --from-file=s3-credentials.txt
kubectl -n $NS get secret s3-credentials -o yaml
cat > secret-ceph-credentials.yaml << 'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: ceph-s3-credentials
type: Opaque
stringData:
  access-key: <exemple-access-key>
  secret-key: <exemple-secret-key>
  endpoint: ceph-rgw.ceph-storage.svc.cluster.local:7480
  bucket: data-platform-bronze
EOF
kubectl -n $NS apply -f secret-ceph-credentials.yaml
kubectl -n $NS get secret ceph-s3-credentials -o yaml
kubectl -n $NS get secret ceph-s3-credentials -o jsonpath='{.data.secret-key}' | base64 -d; echo
kubectl -n $NS get secret ceph-s3-credentials -o json | jq -r '.data | to_entries[] | "\(.key)=\(.value | @base64d)"'
# --- 4. Injection dans des pods (resources ajoutees pour le quota Onyxia)
cat > pod-with-configmap.yaml << 'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: spark-app
spec:
  containers:
  - name: spark
    image: apache/spark:latest
    command: ["sleep", "600"]
    resources:
      requests: {cpu: 20m, memory: 64Mi}
      limits: {cpu: 200m, memory: 256Mi}
    env:
    - name: LOG_LEVEL
      valueFrom:
        configMapKeyRef:
          name: data-platform-config
          key: log_level
    - name: SPARK_WORKERS
      valueFrom:
        configMapKeyRef:
          name: data-platform-config
          key: spark_workers
    envFrom:
    - configMapRef:
        name: data-platform-config
EOF
kubectl -n $NS apply -f pod-with-configmap.yaml
kubectl -n $NS wait --for=condition=Ready pod/spark-app --timeout=300s
kubectl -n $NS exec spark-app -- env | grep -E "LOG_LEVEL|SPARK|log_level|spark_workers"
cat > pod-with-secret.yaml << 'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: ceph-connector
spec:
  restartPolicy: Never
  containers:
  - name: ceph-app
    image: python:3.9
    command: ["python", "-c", "import os; print(f'Access Key: {os.environ[\"S3_ACCESS_KEY\"]}')"]
    resources:
      requests: {cpu: 20m, memory: 32Mi}
      limits: {cpu: 200m, memory: 128Mi}
    env:
    - name: S3_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: ceph-s3-credentials
          key: access-key
    - name: S3_SECRET_KEY
      valueFrom:
        secretKeyRef:
          name: ceph-s3-credentials
          key: secret-key
    - name: S3_ENDPOINT
      valueFrom:
        secretKeyRef:
          name: ceph-s3-credentials
          key: endpoint
    envFrom:
    - secretRef:
        name: ceph-s3-credentials
EOF
kubectl -n $NS apply -f pod-with-secret.yaml
for i in $(seq 1 30); do p=$(kubectl -n $NS get pod ceph-connector -o jsonpath='{.status.phase}'); [ "$p" = "Succeeded" -o "$p" = "Failed" ] && break; sleep 5; done
kubectl -n $NS get pod ceph-connector
kubectl -n $NS logs ceph-connector
cat > pod-with-configmap-volume.yaml << 'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: spark-config-reader
spec:
  containers:
  - name: spark
    image: apache/spark:latest
    command: ["sleep", "600"]
    resources:
      requests: {cpu: 20m, memory: 64Mi}
      limits: {cpu: 200m, memory: 256Mi}
    volumeMounts:
    - name: config-volume
      mountPath: /etc/config
  volumes:
  - name: config-volume
    configMap:
      name: data-platform-config
      items:
      - key: application.yaml
        path: application.yaml
      - key: table_mappings.csv
        path: tables.csv
EOF
kubectl -n $NS apply -f pod-with-configmap-volume.yaml
kubectl -n $NS wait --for=condition=Ready pod/spark-config-reader --timeout=300s
kubectl -n $NS exec spark-config-reader -- ls -la /etc/config/
kubectl -n $NS exec spark-config-reader -- cat /etc/config/application.yaml
kubectl -n $NS exec spark-config-reader -- cat /etc/config/tables.csv
cat > pod-with-secret-volume.yaml << 'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: ceph-connector-secure
spec:
  containers:
  - name: ceph-app
    image: python:3.9
    command: ["sh", "-c", "cat /etc/ceph-credentials/s3-credentials.txt && sleep 1000"]
    resources:
      requests: {cpu: 20m, memory: 32Mi}
      limits: {cpu: 200m, memory: 128Mi}
    volumeMounts:
    - name: ceph-creds
      mountPath: /etc/ceph-credentials
      readOnly: true
  volumes:
  - name: ceph-creds
    secret:
      secretName: s3-credentials
      defaultMode: 0600
EOF
kubectl -n $NS apply -f pod-with-secret-volume.yaml
kubectl -n $NS wait --for=condition=Ready pod/ceph-connector-secure --timeout=300s
kubectl -n $NS exec ceph-connector-secure -- cat /etc/ceph-credentials/s3-credentials.txt
kubectl -n $NS exec ceph-connector-secure -- ls -la /etc/ceph-credentials/
# --- Teardown (pas de namespace dedie : on supprime les objets un par un)
kubectl -n $NS delete pod spark-app ceph-connector spark-config-reader ceph-connector-secure --wait=false
kubectl -n $NS delete configmap pipeline-config data-paths data-platform-config
kubectl -n $NS delete secret ceph-credentials s3-credentials ceph-s3-credentials
cd .. && rm -r lab-configmap-secret
true
