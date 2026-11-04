aws configure set aws_access_key_id "$AWS_ACCESS_KEY_ID" --profile default
aws configure set aws_secret_access_key "$AWS_SECRET_ACCESS_KEY" --profile default
aws configure set aws_session_token "$AWS_SESSION_TOKEN" --profile default
aws configure set region us-east-1 --profile default
aws configure set endpoint_url "https://$AWS_S3_ENDPOINT" --profile default
pgrep -f labsrv.py >/dev/null || (nohup python3 ~/work/labkit/labsrv.py >/tmp/labsrv.log 2>&1 &)
