import json
import sys

json_file = open(sys.argv[1], 'r')
catalog = json.load(json_file)

def get_azure_subscription(dataset):
    for distribution in dataset.get('distribution', []):
        title = distribution.get('title', '')
        if title.endswith('-azure-mirror-sub'):
            return title
    return None

for dataset in catalog.get('dataset'):
    for distribution in dataset.get('distribution'):
        if distribution.get('format') == "azure-event-hub-mirror":
            body = {
                "project_id": catalog.get('projectId'),
                "subscription_name": get_azure_subscription(dataset),
                "event_hub_name": distribution.get('title')
            }
            if body.get('event_hub_name'):
                print(json.dumps(body))
