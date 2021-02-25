import os
import json
import utils
import base64
import logging

from gobits import Gobits
from azure.eventhub import EventHubProducerClient, EventData

logging.basicConfig(level=logging.INFO)

# Creating the Azure Event Hub Producer Client
event_hub_shared_access_key = utils.get_secret(
    os.environ.get('PROJECT_ID'), os.environ.get('EVENTHUB_ACCESS_KEY_SECRET'))
event_hub_shared_access_key_name = os.environ.get('EVENTHUB_ACCESS_KEY_NAME')
event_hub_fqdn = os.environ.get('EVENTHUB_FQDN')
event_hub_name = os.environ.get('EVENTHUB_NAME')

event_hub_connection_string = "Endpoint=sb://{}/;SharedAccessKeyName={};SharedAccessKey={}".format(
    event_hub_fqdn, event_hub_shared_access_key_name, event_hub_shared_access_key)

producer = EventHubProducerClient.from_connection_string(
    conn_str=event_hub_connection_string, eventhub_name=event_hub_name)


def push_to_eventhub(request):
    """
    Unpack Pub/Sub request and process message.
    """

    envelope = json.loads(request.data.decode('utf-8'))
    payload = base64.b64decode(envelope['message']['data'])

    try:
        subscription = envelope['subscription'].split('/')[-1]
        logging.info(f'Message received from {subscription}')

        publish_data(json.loads(payload), request)
    except Exception as e:
        logging.error(f"Extraction of subscription failed: {str(e)}")
        return 'Service Unavailable', 503

    return 'No Content', 204


def publish_data(msg, request):
    """
    Function to publish message towards the Azure Event Hub.
    """

    gobits = msg.get('gobits', [])
    gobits.append(Gobits.from_request(request=request).to_json())
    msg['gobits'] = gobits

    batch = producer.create_batch()
    batch.add(EventData(
        json.dumps(msg)
    ))

    logging.info(f"Publishing message towards Azure Event Hub '{event_hub_name}'")
    producer.send_batch(batch)
