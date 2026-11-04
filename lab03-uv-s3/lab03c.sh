# Lab 03 / lab-2 - fin : nouvelle execution du Job d'upload (la 1re a ete bloquee par le quota de 8 pods, le Job avait quand meme reussi)
cd /home/onyxia/work/$GIT_REPO_NAME
kubectl get pods
kubectl create configmap datasets --from-file=users.csv --from-file=orders.csv
kubectl create configmap s3-config --from-literal=AWS_ENDPOINT_URL="$S3_ENDPOINT_URL" --from-literal=AWS_DEFAULT_REGION="$(aws configure get region --profile 'default')" --from-literal=LAB_BUCKET_NAME="$LAB_BUCKET_NAME"
set +x
aws configure export-credentials --profile 'default' --format env-no-export | grep -E '^AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)=' > s3.env
set -x
kubectl create secret generic s3-credentials --from-env-file=s3.env
rm s3.env
cat job-upload-bronze.yaml
kubectl apply -f job-upload-bronze.yaml
kubectl wait --for=condition=complete job/upload-bronze --timeout=180s
kubectl get job upload-bronze
kubectl logs job/upload-bronze
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/" --recursive
kubectl delete -f job-upload-bronze.yaml
kubectl delete configmap datasets s3-config
kubectl delete secret s3-credentials
rm -f job-upload-bronze.yaml
kubectl get pods
true
