gcloud functions deploy ${PROJECT_ID}-backup-func \
  --entry-point=handler \
  --runtime=python37 \
  --trigger-http \
  --project=${PROJECT_ID} \
  --region=europe-west1 \
  --memory=2048MB \
  --timeout=540s \
  --env-vars-file=env.yaml
