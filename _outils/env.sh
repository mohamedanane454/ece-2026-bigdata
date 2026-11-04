export GIT_REPO_NAME=ece-2026-bigdata
export UV_PROJECT_NAME=ece_2026_bigdata
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export PATH="$HOME/.local/bin:$PATH"
export S3_ENDPOINT_URL=$(aws configure get endpoint_url --profile default || echo "https://$AWS_S3_ENDPOINT")
export REPO=/home/onyxia/work/$GIT_REPO_NAME
export GIT_TERMINAL_PROMPT=0
gpush() { echo "[git push differe : il sera fait a la fin, avec l'authentification GitHub de l'utilisateur]"; }
export -f gpush
