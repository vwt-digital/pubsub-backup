import os
import utils
import logging

from google.cloud import pubsub_v1
from azure.eventhub import EventHubProducerClient, EventData

logging.basicConfig(level=logging.INFO)

producer = None
event_data_batch = None


def callback(msg):
    """
    Callback function for pub/sub subscriber.
    """

    global event_data_batch
    global producer

    msg_json = msg.data.decode()
    event_data_batch.add(EventData(msg_json))
    logging.info(f"Sending {event_data_batch.size_in_bytes} bytes of messages...")
    producer.send_batch(event_data_batch)
    event_data_batch = producer.create_batch()

    msg.ack()


def handler(request):
    """
    Handler function that subscribes to gcp pub/sub
    and sends messages to azure event hub.
    """

    global event_data_batch
    global producer

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
    event_data_batch = producer.create_batch()

    logging.info("Creating GCP subscriber...")
    subscriber = pubsub_v1.SubscriberClient()
    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=callback)

    logging.info(f"Listening for messages on {subscription_path}...")

    with subscriber:
        try:
            streaming_pull_future.result(timeout=30)
        except Exception as e:
            streaming_pull_future.cancel()
            print(f"Listening for messages on {subscription_path} threw an exception: {e}.")

    if event_data_batch.size_in_bytes > 0:
        logging.info(f"Sending {event_data_batch.size_in_bytes} bytes of messages...")
        producer.send_batch(event_data_batch)

    subscriber.close()
    producer.close()


if __name__ == '__main__':
    handler(None)
