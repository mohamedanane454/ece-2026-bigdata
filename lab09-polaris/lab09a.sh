# Lab 09 / lab-1 - Deploiement de Polaris avec Helm
# Prerequis "patch du statefulset vscode" : applique (requests 50m/200Mi, limits 2 CPU / 3000Mi au lieu de 2500Mi pour Spark)
kubectl get pod vscode-pyspark-123865-0 -o jsonpath='{.spec.containers[0].resources}'; echo
kubectl describe quota | tail -7
mkdir -p /home/onyxia/work/polaris-lab && cd /home/onyxia/work/polaris-lab
base64 -d /home/onyxia/work/labkit/polaris_files.b64 | gunzip > /home/onyxia/work/labkit/polaris_files.sh
wc -l /home/onyxia/work/labkit/polaris_files.sh
bash /home/onyxia/work/labkit/polaris_files.sh
find polaris -type f | sort
cd polaris
export POLARIS_URI="http://polaris:8181"
export POLARIS_MGMT_URI="http://polaris-mgmt:8182"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
export API="$POLARIS_URI/api/management/v1"
echo "$LAB_BUCKET_NAME"
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
kubectl create configmap polaris-config --from-literal=POLARIS_CATALOG="$POLARIS_CATALOG" --from-literal=POLARIS_URI="$POLARIS_URI" --from-literal=POLARIS_MGMT_URI="$POLARIS_MGMT_URI" --from-literal=POLARIS_REALM="POLARIS" --from-literal=POLARIS_CLIENT_ID="root" --from-literal=POLARIS_BASE_LOCATION="s3://$LAB_BUCKET_NAME/warehouse/" --from-literal=S3_ENDPOINT="https://s3.seaweedfs.adm.adaltas.cloud/" --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic polaris-credential --from-literal=POLARIS_ROOT_SECRET="s3cr3t" --dry-run=client -o yaml | kubectl apply -f -
set +x
kubectl create secret generic polaris-s3 --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" --from-literal=AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN" --dry-run=client -o yaml | kubectl apply -f -
set -x
helm repo add polaris https://downloads.apache.org/polaris/helm-chart
helm repo update
helm dependency update ./deployment
helm upgrade --install polaris ./deployment --wait --timeout 10m
kubectl get pods
helm list
for i in $(seq 1 30); do curl -s http://polaris-mgmt:8182/q/health | grep -q '"UP"' && break; sleep 5; done
curl -s http://polaris-mgmt:8182/q/health | head -5
kubectl logs job/polaris-bootstrap | grep -v '"credentials"'
set +x
kubectl logs job/polaris-bootstrap | grep '"credentials"' | jq -r 'select(.principal == "dev" or .principal == "analyst") | "POLARIS_\(.principal | ascii_upcase)_CREDENTIAL=\"\(.credentials.clientId):\(.credentials.clientSecret)\""' > /home/onyxia/work/polaris-lab/.polaris-credentials.env
cp /home/onyxia/work/polaris-lab/.polaris-credentials.env /home/onyxia/.polaris-credentials.env
set -a; . /home/onyxia/.polaris-credentials.env; set +a
echo "POLARIS_DEV_CREDENTIAL=${POLARIS_DEV_CREDENTIAL:0:4}...(masque)"
echo "POLARIS_ANALYST_CREDENTIAL=${POLARIS_ANALYST_CREDENTIAL:0:4}...(masque)"
TOKEN=$(curl -s "$POLARIS_URI/api/catalog/v1/oauth/tokens" -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
echo "TOKEN root obtenu : ${#TOKEN} caracteres"
for path in catalogs principals principal-roles "catalogs/$POLARIS_CATALOG/catalog-roles"; do
  echo "\$ curl -s \"\$API/$path\" -H \"Authorization: Bearer \$TOKEN\" | jq"
  curl -s "$API/$path" -H "Authorization: Bearer $TOKEN" | jq -c '(.catalogs // .principals // .roles) | map({name, type, base: .properties["default-base-location"]})'
done
set -x
true
