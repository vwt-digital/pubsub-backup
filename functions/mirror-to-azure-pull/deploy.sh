#!/bin/bash
# shellcheck disable=SC2140

PROJECT_ID=${1}

function error_exit() {
  # ${BASH_SOURCE[1]} is the file name of the caller.
  echo "${BASH_SOURCE[1]}: line ${BASH_LINENO[0]}: ${1:-Unknown Error.} (exit ${2:-1})" 1>&2
  exit "${2:-1}"
}

[[ -n "${PROJECT_ID}" ]] || error_exit "Missing required PROJECT_ID"


gcloud functions deploy "${PROJECT_ID}-azure-mirror-func" \
  --entry-point=handler \
  --runtime=python37 \
  --trigger-http \
  --project="${PROJECT_ID}" \
  --region=europe-west1 \
  --memory=256MB \
  --timeout=30s \
  --set-env-vars=PROJECT_ID="${PROJECT_ID}",CONNECTION_SECRET="${PROJECT_ID}-azure-conn-str",EVENTHUB_SECRET="${PROJECT_ID}-eventhub-name"

gcloud functions add-iam-policy-binding "${PROJECT_ID}-azure-mirror-func" \
  --region=europe-west1 \
  --role=roles/cloudfunctions.invoker \
  --member="serviceAccount:${PROJECT_ID}@appspot.gserviceaccount.com" \
  --project="${PROJECT_ID}"
