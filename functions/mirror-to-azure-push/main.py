import os
import json
import utils
import base64
import logging

from azure.eventhub import EventHubProducerClient, EventData

logging.basicConfig(level=logging.INFO)

# Creating the Azure Event Hub Producer Client
event_hub_connection_string = utils.get_secret(os.environ.get('PROJECT_ID'), os.environ.get('CONNECTION_SECRET'))
event_hub_name = os.environ.get('EVENTHUB_NAME')
producer = EventHubProducerClient.from_connection_string(
    conn_str=event_hub_connection_string, eventhub_name=event_hub_name)


def mirror_to_azure_push(request):
    """
    Unpack Pub/Sub request and process message.
    """

    envelope = json.loads(request.data.decode('utf-8'))
    payload = base64.b64decode(envelope['message']['data'])

    try:
        subscription = envelope['subscription'].split('/')[-1]
        logging.info(f'Message received from {subscription}')

        publish_to_eventhub(msg=json.loads(payload), subscription=subscription)
    except Exception as e:
        logging.error(f"Extraction of subscription failed: {str(e)}")
        return 'Service Unavailable', 503

    return 'No Content', 204


def publish_to_eventhub(msg, subscription):
    """
    Function to publish message towards the Azure Event Hub.
    """

    event = {
        "message": msg,
        "subscription": subscription
    }

    data = json.dumps(event)
    batch = producer.create_batch()
    batch.add(EventData(data))

    logging.info(f"Publishing message towards Azure Event Hub '{event_hub_name}'")
    producer.send_batch(batch)
