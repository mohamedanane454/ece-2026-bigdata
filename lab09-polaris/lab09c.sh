# Lab 09 / lab-3 - Mise a jour des credentials S3 dans le pod Polaris
# Les identifiants temporaires d'Onyxia sont relus depuis le service (equivalent du renouvellement du lab S3)
bash /home/onyxia/work/labkit/setup.sh
eval "$(aws configure export-credentials --profile default --format env)"
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
set +x
kubectl create secret generic polaris-s3 --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" --from-literal=AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN" --dry-run=client -o yaml | kubectl apply -f -
set -x
kubectl rollout restart deployment/polaris
kubectl rollout status deployment/polaris --timeout=300s
kubectl get pods
kubectl exec deployment/polaris -- sh -c 'tr "\0" "\n" < /proc/1/environ | grep AWS_ACCESS_KEY_ID'
echo "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID"
curl -s http://polaris-mgmt:8182/q/health | head -3
true
