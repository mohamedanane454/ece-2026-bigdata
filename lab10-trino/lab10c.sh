# Lab 10 / lab-3 - Star schema
cd /home/onyxia/work/$GIT_REPO_NAME
export TRINO_HOST=trino
export TRINO_PORT=8080
export TRINO_USER=$KUBERNETES_NAMESPACE
uv run python -u /home/onyxia/work/labkit/cells.py /home/onyxia/work/labkit/lab10c.py
mkdir -p lab_trino
aws s3 --profile default sync "s3://$LAB_BUCKET_NAME/warehouse/gold" "./lab_trino/gold/" --exclude "*/data/*"
find lab_trino -type f | sed 's#/[^/]*$##' | sort | uniq -c
git add lab_trino
git commit -m "feat: trino gold metadata"
git log --oneline | head -3
true
