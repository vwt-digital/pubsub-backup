# Mirror to Azure Push function

This function is used to publish Pub/Sub subscription messages towards an Azure Event Hub instances.

## Configuration
These variables have to be defined within the environment of the function:
- `PROJECT_ID` `[string]`: The Project ID of the current GCP project;
- `EVENTHUB_ACCESS_KEY_SECRET` `[string]`: The ID of the Secret Manager secret where the Azure Event Hub access key is 
  defined;
- `EVENTHUB_ACCESS_KEY_NAME` `[string]`: The name of the Azure Event Hub access key;
- `EVENTHUB_NAME` `[string]`: The name of the Azure Event Hub instance.

## Invoking
The function can be invoked by creating a Pub/Sub Push Subscription towards the HTTP-endpoint of the function. Don't
forget to ensure the Pub/Sub instances has Function Invoking permission.

Function entrypoint: `mirror_to_azure_push`

## License
[GPL-3](https://www.gnu.org/licenses/gpl-3.0.en.html)
