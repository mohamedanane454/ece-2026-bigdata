# Lab 03 / lab-2 - Object storage avec S3
# Profil 'default' configure a partir des variables AWS_* du service (equivalent du script "S3 Profile" d'Onyxia)
aws configure list --profile 'default'
echo "$S3_ENDPOINT_URL"
echo "$LAB_BUCKET_NAME"
aws s3 --profile 'default' ls
CMD='export PATH="$HOME/.local/bin:$PATH"'
grep -qxF "$CMD" "$HOME/.bashrc" || echo "$CMD" >> "$HOME/.bashrc"
(
  set -e
  S5CMD_VERSION=$(curl -s https://api.github.com/repos/peak/s5cmd/releases/latest | jq -r '.tag_name | .[1:]')
  BIN_DIR=~/.local/bin
  mkdir -p "$BIN_DIR"
  case "$(uname -m)" in
    x86_64) S5CMD_ARCH="64bit" ;;
    aarch64|arm64) S5CMD_ARCH="arm64" ;;
  esac
  S5CMD_FILE="s5cmd_${S5CMD_VERSION}_Linux-${S5CMD_ARCH}.tar.gz"
  S5CMD_BASE_URL="https://github.com/peak/s5cmd/releases/download/v${S5CMD_VERSION}"
  TMP_DIR=$(mktemp -d)
  cd "$TMP_DIR"
  curl -fsSLO "$S5CMD_BASE_URL/$S5CMD_FILE"
  curl -fsSLO "$S5CMD_BASE_URL/s5cmd_checksums.txt"
  grep " ${S5CMD_FILE}$" s5cmd_checksums.txt | sha256sum -c
  tar -xf "$S5CMD_FILE" -C "$BIN_DIR" s5cmd
  chmod +x "$BIN_DIR/s5cmd"
  rm -rf "$TMP_DIR"
)
command -v s5cmd
s5cmd version
cd /home/onyxia/work/$GIT_REPO_NAME
# --- Upload
uv run dataset-users -o csv > users.csv
aws s3 --profile 'default' cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv"
echo '*.csv' >> .gitignore
# --- List
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
s5cmd --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/" --recursive
# --- Download
aws s3 --profile 'default' cp "s3://$LAB_BUCKET_NAME/bronze/users.csv" ./users_downloaded.csv
diff users.csv users_downloaded.csv && echo "identical"
# --- Move / rename
aws s3 --profile 'default' mv "s3://$LAB_BUCKET_NAME/bronze/users.csv" "s3://$LAB_BUCKET_NAME/bronze/users_renamed.csv"
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
# --- Delete
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/bronze/users_renamed.csv"
# --- Metadata
aws s3 --profile 'default' cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv"
aws s3api --profile 'default' head-object --bucket "$LAB_BUCKET_NAME" --key bronze/users.csv
md5sum users.csv
aws s3 --profile 'default' cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv" --metadata "source=dataset_users.py,version=1,rows=50"
aws s3api --profile 'default' head-object --bucket "$LAB_BUCKET_NAME" --key bronze/users.csv | jq '.Metadata'
# --- Presigned URLs (on n'affiche pas l'URL signee complete)
URL=$(aws s3 --profile 'default' presign "s3://$LAB_BUCKET_NAME/bronze/users.csv" --expires-in 300)
echo "$URL" | cut -d'?' -f1
curl -s "$URL" | head -n 3
uv run --with boto3 python - <<'PY' | cut -d'?' -f1
import os
import boto3

session = boto3.Session(profile_name="default")
s3 = session.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"])
print(s3.generate_presigned_url(
    "put_object",
    Params={"Bucket": os.environ["LAB_BUCKET_NAME"], "Key": "bronze/upload.csv"},
    ExpiresIn=300,
))
PY
# --- Multipart upload
dd if=/dev/urandom of=large_dataset.bin bs=1M count=200
aws s3 --profile 'default' cp large_dataset.bin "s3://$LAB_BUCKET_NAME/large/dataset.bin"
aws s3api --profile 'default' head-object --bucket "$LAB_BUCKET_NAME" --key large/dataset.bin | jq -r '.ETag'
aws s3api --profile 'default' list-multipart-uploads --bucket "$LAB_BUCKET_NAME"
# --- Versioning
aws s3api --profile 'default' put-bucket-versioning --bucket "$LAB_BUCKET_NAME" --versioning-configuration Status=Enabled
aws s3 --profile 'default' cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv"
uv run dataset-users -c 40 -o csv > users_v2.csv
aws s3 --profile 'default' cp users_v2.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv"
aws s3api --profile 'default' list-object-versions --bucket "$LAB_BUCKET_NAME" --prefix bronze/users.csv | jq '.Versions[] | {VersionId, IsLatest, Size, LastModified}'
VID=$(aws s3api --profile 'default' list-object-versions --bucket "$LAB_BUCKET_NAME" --prefix bronze/users.csv | jq -r '[.Versions[] | select(.Size == 7351 or .IsLatest == false)] | sort_by(.LastModified) | .[0].VersionId')
echo "VID=$VID"
aws s3api --profile 'default' get-object --bucket "$LAB_BUCKET_NAME" --key bronze/users.csv --version-id "$VID" users_v1.csv
diff -q users.csv users_v1.csv && echo "version 1 restored"
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/bronze/users.csv"
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
aws s3api --profile 'default' list-object-versions --bucket "$LAB_BUCKET_NAME" --prefix bronze/users.csv
# --- Lifecycle
cat > lifecycle.json <<'EOF'
{
  "Rules": [
    {"ID": "expire-large-after-30-days", "Status": "Enabled", "Filter": { "Prefix": "large/" }, "Expiration": { "Days": 30 }},
    {"ID": "delete-noncurrent-versions", "Status": "Enabled", "Filter": { "Prefix": "" }, "NoncurrentVersionExpiration": { "NoncurrentDays": 7 }},
    {"ID": "abort-incomplete-multipart", "Status": "Enabled", "Filter": { "Prefix": "" }, "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 }}
  ]
}
EOF
aws s3api --profile 'default' put-bucket-lifecycle-configuration --bucket "$LAB_BUCKET_NAME" --lifecycle-configuration file://lifecycle.json
aws s3api --profile 'default' get-bucket-lifecycle-configuration --bucket "$LAB_BUCKET_NAME"
# --- Access control
cat > policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "ProtectBronzeLayer", "Effect": "Deny", "Principal": "*", "Action": ["s3:DeleteObject"], "Resource": "arn:aws:s3:::$LAB_BUCKET_NAME/bronze/*"}
  ]
}
EOF
aws s3 --profile 'default' cp users.csv "s3://$LAB_BUCKET_NAME/bronze/protected.csv"
aws s3api --profile 'default' put-bucket-policy --bucket "$LAB_BUCKET_NAME" --policy file://policy.json
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/bronze/protected.csv"
aws s3api --profile 'default' delete-bucket-policy --bucket "$LAB_BUCKET_NAME"
aws s3api --profile 'default' get-object-acl --bucket "$LAB_BUCKET_NAME" --key bronze/protected.csv
# --- Consistency
aws s3 --profile 'default' cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv"
aws s3 --profile 'default' cp "s3://$LAB_BUCKET_NAME/bronze/users.csv" /dev/null && echo "immediately available"
# --- Cleanup
for prefix in bronze/ large/; do
  aws s3api --profile 'default' list-object-versions --bucket "$LAB_BUCKET_NAME" --prefix "$prefix" --output json | jq -c '{Objects: ([.Versions[]?, .DeleteMarkers[]?] | map({Key, VersionId})), Quiet: true}' > delete.json
  if [ "$(jq '.Objects | length' delete.json)" -gt 0 ]; then
    aws s3api --profile 'default' delete-objects --bucket "$LAB_BUCKET_NAME" --delete file://delete.json
  fi
done
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/bronze/" --recursive
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/large/" --recursive
aws s3api --profile 'default' put-bucket-versioning --bucket "$LAB_BUCKET_NAME" --versioning-configuration Status=Suspended
aws s3api --profile 'default' delete-bucket-lifecycle --bucket "$LAB_BUCKET_NAME"
rm -f large_dataset.bin users_downloaded.csv users_v1.csv users_v2.csv delete.json lifecycle.json policy.json
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/" --recursive
# --- Upload des datasets depuis un Job Kubernetes
kubectl auth can-i create jobs
kubectl auth can-i create secrets
uv run dataset-users -o csv > users.csv
uv run dataset-orders -o csv > orders.csv
ls -lh users.csv orders.csv
kubectl create configmap datasets --from-file=users.csv --from-file=orders.csv
kubectl create configmap s3-config --from-literal=AWS_ENDPOINT_URL="$S3_ENDPOINT_URL" --from-literal=AWS_DEFAULT_REGION="$(aws configure get region --profile 'default')" --from-literal=LAB_BUCKET_NAME="$LAB_BUCKET_NAME"
set +x
aws configure export-credentials --profile 'default' --format env-no-export | grep -E '^AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)=' > s3.env
set -x
kubectl create secret generic s3-credentials --from-env-file=s3.env
rm s3.env
cat > job-upload-bronze.yaml <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: upload-bronze
spec:
  backoffLimit: 2
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: upload
          image: amazon/aws-cli:latest
          command: ["sh", "-c"]
          args:
            - |
              set -e
              for dataset in users orders; do
                aws s3 cp "/data/$dataset.csv" "s3://$LAB_BUCKET_NAME/bronze/$dataset.csv"
              done
              aws s3 ls "s3://$LAB_BUCKET_NAME/bronze/" --recursive
          envFrom:
            - configMapRef:
                name: s3-config
            - secretRef:
                name: s3-credentials
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: 500m
              memory: 256Mi
          volumeMounts:
            - name: datasets
              mountPath: /data
              readOnly: true
      volumes:
        - name: datasets
          configMap:
            name: datasets
EOF
kubectl apply -f job-upload-bronze.yaml
kubectl wait --for=condition=complete job/upload-bronze --timeout=240s
kubectl logs job/upload-bronze
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/" --recursive
kubectl delete -f job-upload-bronze.yaml
kubectl delete configmap datasets s3-config
kubectl delete secret s3-credentials
git status --short
git add .gitignore
git commit -m "chore: ignore csv datasets"
true
