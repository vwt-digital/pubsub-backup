gcloud functions deploy "${PROJECT_ID}-history-func" \
  --entry-point=handler \
  --runtime=python38 \
  --trigger-http \
  --project="${PROJECT_ID}" \
  --region=europe-west1 \
  --memory=512MB \
  --timeout=540s \
  --set-env-vars=PROJECT_ID="${PROJECT_ID}",BRANCH_NAME="${BRANCH_NAME}"
