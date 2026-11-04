# Lab 09 / lab-3 - verification apres scale 0 -> 1 (le rollout restart etait bloque par le quota memoire)
sed -i -E 's/(AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN)([= ]).*/\1\2***masque***/' /home/onyxia/work/labkit/logs/lab09c.log
cat /home/onyxia/work/labkit/logs/lab09c-scale.log
kubectl rollout status deployment/polaris --timeout=300s
kubectl get pods
kubectl exec deployment/polaris -c polaris -- sh -c 'tr "\0" "\n" < /proc/1/environ | grep AWS_ACCESS_KEY_ID'
echo "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID"
for i in $(seq 1 30); do curl -s http://polaris-mgmt:8182/q/health | grep -q '"UP"' && break; sleep 5; done
curl -s http://polaris-mgmt:8182/q/health | head -3
# Les entites survivent au redemarrage (stockees dans PostgreSQL)
TOKEN=$(curl -s http://polaris:8181/api/catalog/v1/oauth/tokens -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
curl -s http://polaris:8181/api/management/v1/principals -H "Authorization: Bearer $TOKEN" | jq -c '[.principals[].name]'
cd /home/onyxia/work/$GIT_REPO_NAME
git status --short
git log --oneline
true
