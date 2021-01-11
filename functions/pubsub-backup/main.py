import os
import sys
import json
import time
import logging
import uuid
import gzip

from datetime import datetime
from google.cloud import storage
from google.cloud import pubsub_v1

from retry import retry

PROJECT_ID = os.getenv('PROJECT_ID')
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
MAX_BYTES = int(os.getenv('MAX_BYTES', '128000000'))
MAX_MESSAGES = int(os.getenv('MAX_MESSAGES', '1000'))
TOTAL_MESSAGES = int(os.getenv('TOTAL_MESSAGES', '250000'))
FUNCTION_TIMEOUT = int(os.getenv('FUNCTION_TIMEOUT', '500'))
MAX_SMALL_BATCHES = int(os.getenv('MAX_SMALL_BATCHES', '10'))

client = pubsub_v1.SubscriberClient()


def handler(request):
    try:
        subscription = request.data.decode('utf-8')
        subscription_path = client.subscription_path(PROJECT_ID, subscription)
        logging.info(f"Starting to archive messages from {subscription_path}...")
        messages, ack_ids = pull_from_pubsub(subscription_path)
    except Exception as e:
        logging.exception(f"Something bad happened, reason: {e}")
        return 'ERROR', 501

    if not messages:
        logging.info("No messages to archive, exiting...")
        return 'OK', 204

    unique_id = str(uuid.uuid4())[:16]
    now = datetime.now()
    epoch = int(now.strftime("%s"))
    prefix = now.strftime('%Y/%m/%d/%H')
    bucket_name = subscription_to_bucket(subscription)

    try:
        messages_string = json.dumps(messages)
        compressed = compress(messages_string)
        to_storage(compressed, bucket_name, prefix, epoch, unique_id)
    except Exception as e:
        logging.exception(f"Storing of file in gs://{bucket_name}/{prefix} failed, reason: {e}")
        return 'ERROR', 501

    try:
        logging.info(f"Going to acknowledge {len(ack_ids)} message(s) from {subscription_path}...")
        chunks = chunk(ack_ids, 1000)
        for batch in chunks:
            client.acknowledge(subscription_path, batch)
            logging.info(f"Acknowledged {len(batch)} message(s)...")
        logging.info(f"Acknowledged {len(ack_ids)} message(s) from {subscription_path}")
    except Exception as e:
        logging.exception(f"Acknowleding failed, reason: {e}")
        return 'ERROR', 501

    return 'OK', 204


def pull_from_pubsub(subscription_path):

    size = 0
    retry = 0
    small = 0
    ack_ids = []
    send_messages = []

    logging.info(f"Starting to gather messages from {subscription_path}...")
    start = time.time()

    while True:
        mail = []

        try:
            resp = client.pull(
                request={
                    "subscription": subscription_path,
                    "max_messages": MAX_MESSAGES
                }, timeout=30.0)
        except Exception as e:
            print(f"Pulling messages on {subscription_path} threw an exception: {e}.")
        else:
            messages = []
            mail = resp.received_messages

            for msg in mail:
                try:
                    message = json.loads(msg.message.data.decode('utf-8'))
                except Exception:
                    logging.warning(f"Json could not be parsed, skipping msg from subscription: {subscription_path}")
                messages.append(message)
                ack_ids.append(msg.ack_id)

        # Retry until max_retries
        if len(mail) == 0:
            retry += 1
            if retry >= MAX_RETRIES:
                logging.info(f"Max retries ({retry}) exceeded, exiting loop..")
                break
            logging.info(f"Found no messages, retry {retry}/{MAX_RETRIES}..")
            continue

        logging.info(f"Appending {len(messages)} message(s)...")
        send_messages.extend(messages)

        # Finish when total messages is reached or time expired
        if len(send_messages) > TOTAL_MESSAGES:
            break
        if (time.time() - start) > FUNCTION_TIMEOUT:
            break

        # Finish when batches become too small
        if len(mail) < 50:
            small += 1
            if small >= MAX_SMALL_BATCHES:
                logging.info("Batches too small, exiting loop..")
                break
            continue
        else:
            small = 0

        # Finish when total messages reaches maximum size in bytes
        size = size + sys.getsizeof(json.dumps(messages))
        if size >= MAX_BYTES:
            logging.info(f"Maximum size of {MAX_BYTES} bytes reached, exiting loop..")
            break

    stop = time.time() - start
    logging.info(f"Finished after {int(stop)} seconds, pulled {len(send_messages)} message(s) from {subscription_path}!")
    return send_messages, ack_ids


@retry(ConnectionError, tries=3, delay=5, backoff=2, logger=None)
def to_storage(blob_bytes, bucket_name, prefix, epoch, unique_id):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
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
    bucket_name = subscription.replace('sub', 'stg')
    return bucket_name


def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
