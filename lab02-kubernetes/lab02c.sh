# Lab 02 / lab-3 - Job & CronJob (namespace personnel, resources ajoutees pour le quota)
kubectl create namespace lab-job-cronjob
NS=$KUBERNETES_NAMESPACE
mkdir -p ~/work/lab-job-cronjob && cd ~/work/lab-job-cronjob
cat > job-simple.yaml <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: data-import-job
spec:
  template:
    spec:
      containers:
      - name: import
        image: python:3.9
        resources: {requests: {cpu: 20m, memory: 32Mi}, limits: {cpu: 200m, memory: 128Mi}}
        command: ["python", "-c"]
        args:
          - |
            import time
            print("Starting data import...")
            time.sleep(3)
            print("Processing 1000 records...")
            print("Import completed!")
      restartPolicy: Never
EOF
kubectl -n $NS apply -f job-simple.yaml
kubectl -n $NS get job
kubectl -n $NS wait --for=condition=complete job/data-import-job --timeout=120s
kubectl -n $NS get job
kubectl logs -n $NS -l job-name=data-import-job
cat > job-with-retry.yaml <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: data-transform-job
spec:
  backoffLimit: 3
  activeDeadlineSeconds: 60
  template:
    spec:
      containers:
      - name: transform
        image: python:3.9
        resources: {requests: {cpu: 20m, memory: 32Mi}, limits: {cpu: 200m, memory: 128Mi}}
        command: ["python", "-c"]
        args:
          - |
            import random
            if random.random() < 0.3:
              print("ERROR: Validation failed!")
              exit(1)
            print("Transformation completed!")
      restartPolicy: Never
EOF
kubectl -n $NS apply -f job-with-retry.yaml
timeout 45 kubectl -n $NS get job data-transform-job -w
kubectl -n $NS get pods -l job-name=data-transform-job
kubectl -n $NS logs -l job-name=data-transform-job --prefix
# --- 2. Parallelisme
cat > job-parallel.yaml <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: batch-export-job
spec:
  parallelism: 4
  completions: 4
  backoffLimit: 2
  template:
    spec:
      containers:
      - name: export
        image: python:3.9
        resources: {requests: {cpu: 20m, memory: 32Mi}, limits: {cpu: 200m, memory: 128Mi}}
        command: ["python", "-c"]
        args:
          - |
            import os, time
            pod = os.getenv('HOSTNAME', 'unknown')
            print(f"Pod {pod}: Exporting shard...")
            time.sleep(5)
            print(f"Pod {pod}: Export completed!")
      restartPolicy: Never
EOF
kubectl -n $NS apply -f job-parallel.yaml
timeout 40 kubectl -n $NS get pods -l job-name=batch-export-job -w
kubectl -n $NS wait --for=condition=complete job/batch-export-job --timeout=120s
kubectl -n $NS get pods -l job-name=batch-export-job -o wide
kubectl -n $NS logs -l job-name=batch-export-job --prefix
# --- 3. CronJob toutes les 2 minutes
cat > cronjob-pipeline.yaml <<'EOF'
apiVersion: batch/v1
kind: CronJob
metadata:
  name: bronze-to-silver-pipeline
spec:
  schedule: "*/2 * * * *"
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 1
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: transform
            image: python:3.9
            resources: {requests: {cpu: 20m, memory: 32Mi}, limits: {cpu: 200m, memory: 128Mi}}
            command: ["python", "-c"]
            args:
              - |
                from datetime import datetime
                print(f"[{datetime.now()}] Bronze->Silver ETL started...")
                import time; time.sleep(2)
                print(f"[{datetime.now()}] Completed! 500 records processed")
          restartPolicy: Never
EOF
kubectl -n $NS apply -f cronjob-pipeline.yaml
kubectl -n $NS get cronjob
timeout 150 kubectl -n $NS get jobs -w
JOB=$(kubectl -n $NS get jobs -o name | grep bronze-to-silver | head -1 | cut -d/ -f2)
echo "JOB=$JOB"
kubectl -n $NS wait --for=condition=complete job/$JOB --timeout=60s
kubectl -n $NS logs -l job-name=$JOB
cat > cronjob-backup.yaml <<'EOF'
apiVersion: batch/v1
kind: CronJob
metadata:
  name: daily-backup
spec:
  schedule: "0 2 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: backup
            image: python:3.9
            resources: {requests: {cpu: 20m, memory: 32Mi}, limits: {cpu: 200m, memory: 128Mi}}
            command: ["python", "-c", "print('Backup completed')"]
          restartPolicy: Never
EOF
kubectl -n $NS apply -f cronjob-backup.yaml
# --- 4. Monitoring
kubectl -n $NS get jobs
kubectl -n $NS describe job data-import-job
kubectl -n $NS logs -l job-name=data-import-job
kubectl -n $NS patch cronjob bronze-to-silver-pipeline -p '{"spec":{"suspend":true}}'
kubectl -n $NS get cronjob bronze-to-silver-pipeline
kubectl -n $NS patch cronjob bronze-to-silver-pipeline -p '{"spec":{"suspend":false}}'
kubectl -n $NS get cronjob bronze-to-silver-pipeline
# --- 5. Scenario pipeline ETL (toutes les 5 minutes)
cat > cronjob-etl.yaml <<'EOF'
apiVersion: batch/v1
kind: CronJob
metadata:
  name: etl-bronze-to-silver
spec:
  schedule: "*/5 * * * *"
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      backoffLimit: 2
      template:
        spec:
          containers:
          - name: etl
            image: python:3.9
            resources: {requests: {cpu: 20m, memory: 32Mi}, limits: {cpu: 200m, memory: 128Mi}}
            env:
            - name: BATCH_SIZE
              value: "1000"
            command: ["python", "-c"]
            args:
              - |
                from datetime import datetime
                import os
                timestamp = datetime.now().isoformat()
                batch_size = os.getenv('BATCH_SIZE')
                print(f"[{timestamp}] ETL: Bronze->Silver")
                print(f"[{timestamp}] Batch size: {batch_size}")
                print(f"[{timestamp}] Reading, validating, deduplicating...")
                print(f"[{timestamp}] Writing to Silver layer...")
                print(f"[{timestamp}] Completed!")
          restartPolicy: Never
EOF
kubectl -n $NS apply -f cronjob-etl.yaml
kubectl get cronjob -n $NS
for i in $(seq 1 40); do J=$(kubectl -n $NS get jobs -o name | grep etl-bronze | head -1); [ -n "$J" ] && break; sleep 10; done
echo "ETL JOB=$J"
kubectl -n $NS wait --for=condition=complete $J --timeout=90s
kubectl -n $NS logs $J
kubectl get jobs -n $NS
# --- Teardown
kubectl -n $NS delete job --all
kubectl -n $NS delete cronjob --all
kubectl -n $NS get jobs,cronjobs
cd .. && rm -r lab-job-cronjob
true
