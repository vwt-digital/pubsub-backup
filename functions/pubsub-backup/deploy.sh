gcloud functions deploy ${PROJECT_ID}-history-func \
  --entry-point=handler \
  --runtime=python37 \
  --trigger-http \
  --project=${PROJECT_ID} \
  --region=europe-west1 \
  --memory=1024MB \
  --timeout=540s \
  --set-env-vars=PROJECT_ID=${PROJECT_ID}
