#!/bin/bash

schedulers=$(gcloud scheduler jobs list --project="${PROJECT_ID}" --format="value(name)" --filter="name:*azure-mirror-sub-job")

for scheduler in $schedulers
do
  gcloud scheduler jobs delete "$scheduler" --project="${PROJECT_ID}" --quiet
done

subscriptions=$(gcloud pubsub subscriptions list --format="value(name)" --project="${PROJECT_ID}" --filter="name:*azure-mirror-sub")

for subscription in $subscriptions
do
  job=$(echo "${subscription}-job" | rev | cut -d '/' -f 1 | rev)
  gcloud scheduler jobs create http "${job}" \
    --schedule="*/5 * * * *" \
    --uri="https://europe-west1-${PROJECT_ID}.cloudfunctions.net/${PROJECT_ID}-azure-mirror-func/" \
    --http-method=POST \
    --oidc-service-account-email="${PROJECT_ID}@appspot.gserviceaccount.com" \
    --oidc-token-audience="https://europe-west1-${PROJECT_ID}.cloudfunctions.net/${PROJECT_ID}-azure-mirror-func" \
    --message-body="${subscription}" \
    --max-retry-attempts 3 \
    --max-backoff 10s \
    --attempt-deadline 10m \
    --project="${PROJECT_ID}"
done
