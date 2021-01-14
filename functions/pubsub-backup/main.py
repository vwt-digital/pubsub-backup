import os
import sys
import json
import time
import logging
import uuid
import gzip
import threading

from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud import exceptions as gcp_exceptions
from datetime import datetime

from retry import retry

PROJECT_ID = os.getenv('PROJECT_ID')
BRANCH_NAME = os.getenv('BRANCH_NAME')
FUNCTION_TIMEOUT = int(os.getenv('FUNCTION_TIMEOUT', '500'))

ps_client = pubsub_v1.SubscriberClient()
stg_client = storage.Client()

messages = []
ack_ids = []

messages_lock = threading.Lock()


def handler(request):
    try:
        subscription = request.data.decode('utf-8')
        subscription_path = ps_client.subscription_path(PROJECT_ID, subscription)
        logging.info(f"Starting to archive messages from {subscription_path}...")
        pull(subscription, subscription_path)
    except Exception as e:
        logging.exception(f"Something bad happened, reason: {e}")
        return 'ERROR', 501

    if not messages:
        logging.info("No messages to archive, exiting...")

    return 'OK', 204


def write_to_file(subscription, messages):
    unique_id = str(uuid.uuid4())[:16]
    now = datetime.now()
    epoch = int(now.timestamp())
    prefix = now.strftime('%Y/%m/%d/%H')
    bucket_name = subscription_to_bucket(subscription)

    try:
        compressed = compress(json.dumps(messages))
        to_storage(compressed, bucket_name, prefix, epoch, unique_id)
    except Exception as e:
        logging.exception(f"Storing of file in gs://{bucket_name}/{prefix} failed, reason: {e}")
        return 'ERROR', 501


def ack(subscription_path, ack_ids):
    try:
        logging.info(f"Going to acknowledge {len(ack_ids)} message(s) from {subscription_path}...")
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
    global ack_ids
    global messages

    messages.clear()
    ack_ids.clear()

    streaming_pull_future = ps_client.subscribe(
                        subscription_path,
                        callback=callback,
                        flow_control=pubsub_v1.types.FlowControl(max_messages=10000))

    logging.info(f"Listening for messages on {subscription_path}...")

    try:
        last_nr_messages = len(messages)
        start = datetime.now()
        time.sleep(5)

        while True:
            # Less than 50 messages stop collecting
            if len(messages)-last_nr_messages < 50:
                streaming_pull_future.cancel()
                break

            # limit the duration of the function
            if (datetime.now() - start).total_seconds() > FUNCTION_TIMEOUT:
                streaming_pull_future.cancel()
                break

            if len(messages) > 5000:
                messages_lock.acquire()
                messages_for_file = messages.copy()
                messages.clear()
                ack_ids_for_file = ack_ids.copy()
                ack_ids.clear()
                messages_lock.release()

                write_to_file(subscription, messages_for_file)
                ack(subscription_path, ack_ids_for_file)

            last_nr_messages = len(messages)
            time.sleep(1)

    except TimeoutError:
        streaming_pull_future.cancel()
    except Exception:
        logging.exception(f"Listening for messages on {subscription_path} threw an exception.")

    # Finally:
    if len(messages) > 0:
        write_to_file(subscription, messages)
        ack(subscription_path, ack_ids)


def callback(msg):
    """
    Callback function for pub/sub subscriber.
    """
    global messages
    global ack_ids

    messages_lock.acquire()

    messages.append(json.loads(msg.data.decode()))
    ack_ids.append(msg.ack_id)

    messages_lock.release()

    if len(messages) % 1000 == 0:
        logging.info("Received {} msgs".format(len(messages)))


@retry(ConnectionError, tries=3, delay=5, backoff=2, logger=None)
def to_storage(blob_bytes, bucket_name, prefix, epoch, unique_id):
    bucket = stg_client.get_bucket(bucket_name)
    blob_name = f"{prefix}/{epoch}-{unique_id}.archive.gz"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(blob_bytes)
    logging.info(f"Uploaded file gs://{bucket_name}/{blob_name}")


def compress(data):
    logging.info(f"The uncompressed size is {sys.getsizeof(data)} bytes")
    compressed = gzip.compress(data.encode())
    logging.info(f"The compressed size is {sys.getsizeof(compressed)} bytes")
    return compressed


def subscription_to_bucket(subscription):
    # TODO: Merge production to staging bucket
    bucket_name = subscription.replace('sub', 'stg')

    # Check if staging bucket exists, otherwise fallback to default backup bucket
    if BRANCH_NAME == "develop" and subscription.endswith('-history-sub'):
        try:
            bucket_staging_name = subscription.replace('-history-sub', '-hst-sa-stg')
            stg_client.get_bucket(bucket_staging_name)
        except gcp_exceptions.NotFound:
            pass
        else:
            bucket_name = bucket_staging_name

    return bucket_name


def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
