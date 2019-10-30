import os
import sys
import json
import logging
import base64
import uuid
import traceback
import gzip

from datetime import datetime
from google.cloud import storage
from google.cloud import pubsub_v1

TOKEN = os.getenv('TOKEN')
MAX_MESSAGES = int(os.getenv('MAX_MESSAGES', '20000'))
PROJECT_ID = os.getenv('PROJECT_ID')


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
        envelope = json.loads(request.data.decode('utf-8'))
        subscription = base64.b64decode(envelope['message']['data']).decode('utf-8')
        logging.info(f"Starting backup of messages from {subscription}")
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Extraction of msg failed, reason: {e}")
        return 'OK', 204

    try:
        messages, ids = pull_from_pubsub(subscription)
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Pulling of messages from {subscription} failed, reason: {e}")
        return 'OK', 204

    if not messages:
        logging.info(f"No messages to backup, exiting..")
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

    acknowledge(subscription, ids)
    return 'OK', 204


def pull_from_pubsub(subscription):
    client = pubsub_v1.SubscriberClient()
    sub_path = client.subscription_path(PROJECT_ID, subscription)
    response = client.pull(sub_path, max_messages=MAX_MESSAGES)

    ack_ids = []
    messages = []
    for msg in response.received_messages:
        message = msg.message.data.decode('utf-8')
        msg_json = json.loads(message)
        messages.append(msg_json)
        ack_ids.append(msg.ack_id)

    logging.info(f"Pulled {len(messages)} from {subscription}")
    return messages, ack_ids


def acknowledge(subscription, ack_ids):
    client = pubsub_v1.SubscriberClient()
    sub_path = client.subscription_path(PROJECT_ID, subscription)
    client.acknowledge(sub_path, ack_ids)
    logging.info(f"Acknowledged {len(ack_ids)} messages on {subscription}")


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
