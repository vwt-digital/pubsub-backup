gcloud functions deploy ${PROJECT_ID}-backup-func \
  --entry-point=handler \
  --runtime=python37 \
  --trigger-http \
  --project=${PROJECT_ID} \
  --region=europe-west1 \
  --env-vars-file=env.yaml
