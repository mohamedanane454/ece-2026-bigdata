# Lab 10 / lab-2 - Silver -> gold avec Trino
cd /home/onyxia/work/$GIT_REPO_NAME
echo "$LAB_BUCKET_NAME"
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
export TRINO_HOST=trino
export TRINO_PORT=8080
export TRINO_USER=$KUBERNETES_NAMESPACE
uv add trino pandas
uv run python -u /home/onyxia/work/labkit/cells.py /home/onyxia/work/labkit/lab10b.py
git add pyproject.toml uv.lock
git commit -m "feat: trino dependency added"
true
