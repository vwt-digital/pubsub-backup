gcloud functions deploy ${PROJECT_ID}-compress-func \
  --entry-point=compress \
  --runtime=python37 \
  --project=${PROJECT_ID} \
  --trigger-http \
  --region=europe-west1 \
  --timeout=540s \
  --memory=2048MB \
  --env-vars-file=env.yaml
