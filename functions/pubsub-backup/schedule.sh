gcloud scheduler jobs create http my_test_schedule \
  --schedule="* 08 * * *" \
  --uri=https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${PROJECT_ID}-backup-func/ \
  --http-method=POST \
  --oidc-service-account-email=${PROJECT_ID}@appspot.gserviceaccount.com \
  --oidc-token-audience=https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${PROJECT_ID}-backup-func \
  --message-body=${BACKUP_SUBSCRIPTION}
