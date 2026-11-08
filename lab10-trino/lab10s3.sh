# Renouvellement des identifiants S3 (profil default mis a jour par le script "S3 profile" d'Onyxia)
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/bronze/"
echo "cle active : ${AWS_ACCESS_KEY_ID:0:8}..."
set +x
kubectl create secret generic polaris-s3 --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" --from-literal=AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN" --dry-run=client -o yaml | kubectl apply -f -
set -x
# Lab 09 / help : redemarrage de Polaris (scale 0 puis 1 a cause du quota)
kubectl scale deployment/polaris --replicas=0
sleep 15
kubectl scale deployment/polaris --replicas=1
kubectl rollout status deployment/polaris --timeout=300s
for i in $(seq 1 30); do curl -s http://polaris-mgmt:8182/q/health | grep -q '"UP"' && break; sleep 5; done
curl -s http://polaris-mgmt:8182/q/health | head -3
kubectl get pods
true
