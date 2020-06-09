import logging
from google.cloud import pubsub_v1
from azure.eventhub import EventHubProducerClient, EventData
import config


logging.basicConfig(level=logging.INFO)

event_data_batch = None
producer = None


def callback_handle_message(msg):
    global event_data_batch
    global producer
    msg_json = msg.data.decode()

    try:
        logging.info(f"Adding eventdata {msg_json}")
        event_data_batch.add(EventData(msg_json))
    except ValueError:
        logging.info(f"Sending {event_data_batch.size_in_bytes} bytes of messages...")
        producer.send_batch(event_data_batch)
        event_data_batch = producer.create_batch()

    msg.ack()


def retrieve_and_mirror_msgs(request):
    global event_data_batch
    global producer
    logging.info(f"Opening Event Hub producer for {config.EVENTHUB_NAME}...")
    producer = EventHubProducerClient.from_connection_string(
           conn_str=config.EVENT_HUB_CONNECT_STRING,
           eventhub_name=config.EVENTHUB_NAME
       )

    logging.info(f"Creating producer batch")
    event_data_batch = producer.create_batch()

    logging.info(f"Opening subscriber on {config.PUBSUB_SUBSCRIPTION_NAME}")
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(config.PUBSUB_PROJECT_ID, config.PUBSUB_SUBSCRIPTION_NAME)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback_handle_message)

    logging.info(f"Listening for messages on {subscription_path}...")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            streaming_pull_future.result(timeout=5)
        except Exception as e:
            streaming_pull_future.cancel()
            print(f"Listening for messages on {subscription_path} threw an exception: {e}.")

    if event_data_batch.size_in_bytes > 0:
        logging.info(f"Sending {event_data_batch.size_in_bytes} bytes of messages...")
        producer.send_batch(event_data_batch)

    subscriber.close()
    producer.close()


if __name__ == '__main__':
    retrieve_and_mirror_msgs(None)
