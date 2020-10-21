import os
import json
import utils
import logging

from google.cloud import pubsub_v1
from azure.eventhub import EventHubProducerClient, EventData

logging.basicConfig(level=logging.INFO)

producer = None
subscription_path = None


def handler(request):
    """
    Handler function that subscribes to gcp pub/sub
    and sends messages to azure event hub.
    """

    global producer
    global subscription_path

    subscription_path = request.data.decode('utf-8')

    event_hub_connection_string = utils.get_secret(
        os.environ['PROJECT_ID'],
        os.environ['CONNECTION_SECRET']
    )

    event_hub_name = utils.get_secret(
        os.environ['PROJECT_ID'],
        os.environ['EVENTHUB_SECRET']
    )

    logging.info("Creating Azure producer...")
    producer = EventHubProducerClient.from_connection_string(
           conn_str=event_hub_connection_string,
           eventhub_name=event_hub_name)

    logging.info("Creating GCP subscriber...")
    subscriber = pubsub_v1.SubscriberClient()
    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=callback)

    logging.info(f"Listening for messages on {subscription_path}...")

    with subscriber:
        try:
            streaming_pull_future.result(timeout=10)
        except Exception as e:
            streaming_pull_future.cancel()
            print(f"Listening for messages on {subscription_path} threw an exception: {e}.")

    producer.close()


def callback(msg):
    """
    Callback function for pub/sub subscriber.
    """

    global producer
    global subscription_path

    event = {
        "message": msg.data.decode(),
        "subscription": subscription_path.split("/")[-1]
    }

    data = json.dumps(event)
    batch = producer.create_batch()
    batch.add(EventData(data))
    logging.info(f"Sending {batch.size_in_bytes} bytes of messages...")
    producer.send_batch(batch)

    msg.ack()
