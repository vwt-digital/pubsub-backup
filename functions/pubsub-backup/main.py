import os
import json
import logging
import base64

from datetime import datetime
from google.cloud import storage

TOKEN = os.getenv('TOKEN')


def consume(request):
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
        event = base64.b64decode(envelope['message']['data'])
        subscription = envelope['subscription'].split('/')[-1]
        id = envelope['message']['message_id']
        publish_time = envelope['message']['publish_time']
        ts = datetime.strptime(publish_time, '%Y-%m-%dT%H:%M:%S.%fZ')

    except Exception as e:
        logging.error(f"Extraction of event failed, reason: {e}")
        return 'OK', 204

    try:
        bucket_name = subscription_to_bucket(subscription)
        to_storage(event, bucket_name, id, ts)
    except Exception as e:
        logging.error(f"Backup of event with {id} failed, reason: {e}")
        return 'OK', 204

    # Returning any 2xx status indicates successful receipt of the message.
    # 204: no content, delivery successfull, no further actions needed
    return 'OK', 204


def to_storage(event, bucket_name, id, ts):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob_name = f"{ts.year}/{ts.month}/{ts.day}/{ts.hour}/{id}-event.json"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(event)
    print('File {} stored in {}.'.format(blob_name, bucket_name))


def subscription_to_bucket(subscription):
    bucket_name = subscription.replace('sub', 'stg')
    return bucket_name
