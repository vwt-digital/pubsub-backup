#!/bin/bash
# shellcheck disable=SC2140

gcloud functions deploy "${PROJECT_ID}-azure-mirror-func" \
  --entry-point=handler \
  --runtime=python37 \
  --trigger-http \
  --project="${PROJECT_ID}" \
  --region=europe-west1 \
  --memory=1024MB \
  --timeout=540s \
  --set-env-vars=PROJECT_ID="${PROJECT_ID}",CONNECTION_SECRET="${PROJECT_ID}-azure-conn-str",EVENTHUB_SECRET="${PROJECT_ID}-eventhub-name"

gcloud functions add-iam-policy-binding "${PROJECT_ID}-azure-mirror-func" \
  --region=europe-west1 \
  --role=roles/cloudfunctions.invoker \
  --member="serviceAccount:${PROJECT_ID}@appspot.gserviceaccount.com" \
  --project="${PROJECT_ID}"
