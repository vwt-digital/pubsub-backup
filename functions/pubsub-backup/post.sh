curl -i -X POST https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${PROJECT_ID}-backup-func/ \
  --data "${BACKUP_SUBSCRIPTION}" \
  -H "Authorization: bearer $(gcloud auth print-identity-token ${PROJECT_ID}@appspot.gserviceaccount.com)"
