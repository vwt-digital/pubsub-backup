#!/bin/bash

DATA_CATALOG=${1}
PROJECT_ID=${2}

function error_exit() {
  # ${BASH_SOURCE[1]} is the file name of the caller.
  echo "${BASH_SOURCE[1]}: line ${BASH_LINENO[0]}: ${1:-Unknown Error.} (exit ${2:-1})" 1>&2
  exit "${2:-1}"
}

[[ -n "${DATA_CATALOG}" ]] || error_exit "Missing required DATA_CATALOG"
[[ -n "${PROJECT_ID}" ]] || error_exit "Missing required PROJECT_ID"

basedir=$(dirname "$0")

curl https://stedolan.github.io/jq/download/linux64/jq > /usr/bin/jq && chmod +x /usr/bin/jq

python3 "${basedir}"/body.py "${DATA_CATALOG}" > body.txt

while read -r body; do
    job="$(echo "$body" | jq -r .subscription_name)-job"
    gcloud scheduler jobs create http "${job}" \
      --schedule="*/5 * * * *" \
      --uri="https://europe-west1-${PROJECT_ID}.cloudfunctions.net/${PROJECT_ID}-azure-mirror-func/" \
      --http-method=POST \
      --oidc-service-account-email="${PROJECT_ID}@appspot.gserviceaccount.com" \
      --oidc-token-audience="https://europe-west1-${PROJECT_ID}.cloudfunctions.net/${PROJECT_ID}-azure-mirror-func" \
      --message-body="${body}" \
      --max-retry-attempts 3 \
      --max-backoff 10s \
      --attempt-deadline 10m \
      --project="${PROJECT_ID}"
done < body.txt
