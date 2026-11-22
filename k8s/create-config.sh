#!/usr/bin/env bash
# Create (or recreate) the ConfigMap and Secret used by k8s/job-upload-bronze.yaml.
# Credentials are piped straight into the Secret and never printed.
# Rerun this script whenever the Onyxia temporary credentials have expired.
set -euo pipefail

export BUCKET="$KUBERNETES_NAMESPACE"
export S3_ENDPOINT_URL=$(aws configure get endpoint_url --profile 'default' || echo "https://$AWS_S3_ENDPOINT")
REGION=$(aws configure get region --profile 'default' || echo "us-east-1")

kubectl delete configmap s3-config --ignore-not-found
kubectl create configmap s3-config \
  --from-literal=AWS_ENDPOINT_URL="$S3_ENDPOINT_URL" \
  --from-literal=AWS_DEFAULT_REGION="$REGION" \
  --from-literal=BUCKET="$BUCKET"

kubectl delete secret s3-credentials --ignore-not-found
kubectl create secret generic s3-credentials \
  --from-env-file=<(aws configure export-credentials --profile 'default' --format env-no-export)

kubectl get configmap s3-config -o jsonpath='{.data}'; echo
kubectl describe secret s3-credentials | grep -E '^(Name|AWS_)'   # key names and sizes only
