import os
import sys
import json
import time
import logging
import uuid
import threading

from google.cloud import storage
from google.cloud import pubsub_v1
from datetime import datetime

from retry import retry

logging.basicConfig(level=logging.INFO)
logging.getLogger('google.cloud.pubsub_v1').setLevel(logging.WARNING)
logging.getLogger('google.api_core').setLevel(logging.WARNING)

PROJECT_ID = os.getenv('PROJECT_ID')
FUNCTION_TIMEOUT = int(os.getenv('FUNCTION_TIMEOUT', '500'))

ps_client = pubsub_v1.SubscriberClient()
stg_client = storage.Client()


def handler(request):

    try:
        subscription = request.data.decode('utf-8')
        subscription_path = ps_client.subscription_path(PROJECT_ID, subscription)
        logging.info(f"Starting to archive messages from {subscription_path}...")
        pull(subscription, subscription_path)
    except Exception as e:
        logging.exception(f"Something bad happened, reason: {e}")
        return 'ERROR', 501

    return 'OK', 204


def write_to_file(subscription, messages):
    unique_id = str(uuid.uuid4())[:16]
    now = datetime.now()
    epoch = int(now.timestamp())
    prefix = now.strftime('%Y/%m/%d/%H')
    bucket_name = subscription_to_bucket(subscription)

    try:
        to_storage(json.dumps(messages), bucket_name, prefix, epoch, unique_id)
    except Exception as e:
        logging.exception(f"Storing of file in gs://{bucket_name}/{prefix} failed, reason: {e}")
        return 'ERROR', 501


def ack(subscription_path, ack_ids):
    try:
        chunks = chunk(ack_ids, 1000)
        for batch in chunks:
            ps_client.acknowledge(
                request={
                    "subscription": subscription_path,
                    "ack_ids": batch
                })
        logging.info(f"Acknowledged {len(ack_ids)} message(s) from {subscription_path}")
    except Exception as e:
        logging.exception(f"Acknowleding failed, reason: {e}")
        return 'ERROR', 501


def pull(subscription, subscription_path):
    messages = []
    ack_ids = []

    messages_lock = threading.Lock()

    # Callback to be called for every single message received
    def callback(msg):
        messages_lock.acquire()
        try:
            messages.append(json.loads(msg.data.decode()))
            ack_ids.append(msg.ack_id)
        finally:
            messages_lock.release()

        if len(messages) % 1000 == 0:
            logging.info("Received {} msgs".format(len(messages)))

    # Callback to be called when the last message has been received (and the async pull finished)
    def done_callback(fut):
        if len(messages) > 0:
            write_to_file(subscription, messages)
            ack(subscription_path, ack_ids)

    streaming_pull_future = ps_client.subscribe(
                        subscription_path,
                        callback=callback,
                        flow_control=pubsub_v1.types.FlowControl(max_messages=75000))

    streaming_pull_future.add_done_callback(done_callback)

    logging.info(f"Listening for messages on {subscription_path}...")

    start = datetime.now()
    time.sleep(5)

    last_nr_messages = 0
    try:
        while True:
            # Less than 25 messages stop collecting
            if len(messages)-last_nr_messages < 25:
                streaming_pull_future.cancel()
                break

            # limit the duration of the function
            if (datetime.now() - start).total_seconds() > FUNCTION_TIMEOUT:
                streaming_pull_future.cancel()
                break

            if sys.getsizeof(json.dumps(messages)) > 5000000:
                messages_lock.acquire()

                try:
                    messages_for_file = messages.copy()
                    messages.clear()
                    ack_ids_for_file = ack_ids.copy()
                    ack_ids.clear()
                finally:
                    messages_lock.release()

                write_to_file(subscription, messages_for_file)
                messages_for_file.clear()

                ack(subscription_path, ack_ids_for_file)
                ack_ids_for_file.clear()

            last_nr_messages = len(messages)
            time.sleep(0.5)

    except TimeoutError:
        streaming_pull_future.cancel()
    except Exception:
        logging.exception(f"Listening for messages on {subscription_path} threw an exception.")

    # Wait until the whole Future is done
    while not streaming_pull_future.done():
        logging.info('=== WAIT ===')
        time.sleep(0.1)


@retry(ConnectionError, tries=3, delay=5, backoff=2, logger=None)
def to_storage(blob_bytes, bucket_name, prefix, epoch, unique_id):
    bucket = stg_client.get_bucket(bucket_name)
    blob_name = f"{prefix}/{epoch}-{unique_id}.json"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(blob_bytes, content_type="application/json")
    logging.info(f"Uploaded file gs://{bucket_name}/{blob_name}")


def subscription_to_bucket(subscription):
    return subscription.replace('-history-sub', '-hst-sa-stg')


def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
