import os
import sys
import json
import time
import logging
import uuid
import traceback
import gzip

from datetime import datetime
from google.cloud import storage
from google.cloud import pubsub_v1

TOKEN = os.getenv('TOKEN')
PROJECT_ID = os.getenv('PROJECT_ID')
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
MAX_MESSAGES = int(os.getenv('MAX_MESSAGES', '1000'))
TOTAL_MESSAGES = int(os.getenv('TOTAL_MESSAGES', '250000'))
FUNCTION_TIMEOUT = int(os.getenv('FUNCTION_TIMEOUT', '520'))


def handler(request):
    # Verify to Google that the function is within your domain
    if request.method == 'GET':
        return f"""
            <html>
                <head>
                    <meta name="google-site-verification" content="{TOKEN}" />
                </head>
                <body>
                </body>
            </html>
        """

    try:
        # Read data from request, origin is a post request
        subscription = request.data.decode('utf-8')
        logging.info(f"Starting backup of messages from {subscription}...")
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Extraction of msg failed, reason: {e}")
        return 'OK', 204

    try:
        messages = pull_from_pubsub(subscription)
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Pulling of messages from {subscription} failed, reason: {e}")
        return 'OK', 204

    if not messages:
        logging.info(f"No messages to backup, exiting...")
        return 'OK', 204

    id = str(uuid.uuid4())[:16]
    now = datetime.now()
    epoch = int(now.strftime("%s"))
    prefix = now.strftime('%Y/%m/%d')
    bucket_name = subscription_to_bucket(subscription)

    try:
        messages_string = json.dumps(messages)
        compressed = compress(messages_string)
        to_storage(compressed, bucket_name, prefix, epoch, id)
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Storing of file in gs://{bucket_name}/{prefix} failed, reason: {e}")
        return 'OK', 204

    return 'OK', 204


def pull_from_pubsub(subscription):
    client = pubsub_v1.SubscriberClient()
    subscription_path = client.subscription_path(PROJECT_ID, subscription)

    retry = 0
    backoff = 0
    send_messages = []

    logging.info(f"Starting to gather messages from {subscription}...")
    start = time.time()

    while True:
        try:
            resp = client.pull(subscription_path, max_messages=MAX_MESSAGES, return_immediately=True)
        except Exception as e:
            logging.error(f"Something went wrong: {e}")
            break

        ack_ids = []
        messages = []
        mail = resp.received_messages

        for msg in mail:
            message = msg.message.data.decode('utf-8')
            messages.append(message)
            ack_ids.append(msg.ack_id)

        # Retry up until max_retries or total_messages
        # Back off when max_messages is not reached
        if len(mail) is not MAX_MESSAGES:
            time.sleep(0.125 * backoff)
            backoff += 1
        else:
            backoff -= 1
        if retry >= MAX_RETRIES:
            print(f"Max retries ({retry}) exceeded, exiting loop..")
            break
        if len(mail) == 0:
            retry += 1
            continue
        if len(messages) > TOTAL_MESSAGES:
            break

        try:
            client.acknowledge(subscription_path, ack_ids)
        except Exception as e:
            logging.error(f"Something went wrong: {e}")
            logging.info('Continuing...')
            continue

        logging.info(f"Appending {len(messages)} messages...")
        send_messages.extend(messages)

        if (time.time() - start) > FUNCTION_TIMEOUT:
            break

    stop = time.time() - start
    logging.info(f"Finished after {int(stop)} seconds, pulled {len(send_messages)} messages from {subscription}!")
    return send_messages


def to_storage(blob_bytes, bucket_name, prefix, epoch, id):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob_name = f"{prefix}/{epoch}-{id}.archive.gz"
    blob = bucket.blob(blob_name)
    blob.cache_control = 'no-cache'
    blob.content_encoding = 'gzip'
    blob.upload_from_string(blob_bytes)
    logging.info(f"Uploaded file gs://{bucket_name}/{blob_name}")


def compress(str):
    logging.info(f"The uncompressed size is {sys.getsizeof(str)} bytes")
    compressed = gzip.compress(str.encode())
    logging.info(f"The compressed size is {sys.getsizeof(compressed)} bytes")
    return compressed


def subscription_to_bucket(subscription):
    bucket_name = subscription.replace('sub', 'stg')
    return bucket_name
