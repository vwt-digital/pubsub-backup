import os
import json
import logging
import uuid
import gzip
import base64

from google.cloud import storage
from datetime import datetime, timedelta

TOKEN = os.getenv('TOKEN')
BACKUP_DELAY = os.getenv('BACKUP_DELAY')
BACKUP_FREQUENCY = os.getenv('BACKUP_FREQUENCY')
storage_client = storage.Client()


def compress(request):
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
        event = base64.b64decode(envelope['message']['data']).decode('utf-8')
        event_json = json.loads(event)
        bucket_name = event_json['bucket']
    except Exception as e:
        logging.error(f"Extraction of event failed, reason: {e}")
        return 'OK', 204

    now = datetime.now()
    backup_period_end = now - get_timedelta(BACKUP_DELAY)
    backup_period_begin = backup_period_end - get_timedelta(BACKUP_FREQUENCY)

    prefix = backup_period_begin.strftime('%Y/%m/%d/%H')
    file_names = list_bucket(bucket_name, prefix)
    file_list = []
    for file_name in file_names:
        file_content = from_storage(bucket_name, file_name)
        if file_content:
            file_list.append(file_content)

    if not file_list:
        logging.info(f"Directory {prefix} in bucket {bucket_name} empty, nothing to do")
        return 'OK', 204

    file_string = ",".join(file_list)
    compressed = gzip.compress(bytes(json.dumps(file_string), encoding="utf-8"))

    id = str(uuid.uuid4())
    to_storage(compressed, bucket_name, prefix, id)

    logging.info('Removing old files...')
    for file_name in file_names:
        delete_old(bucket_name, file_name)
    logging.info('Done archiving!')

    return 'OK', 204


def from_storage(bucket_name, file_name):
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(file_name)
    logging.info(f'Get file {file_name} from {bucket_name}')
    if blob is not None:
        content = blob.download_as_string().decode()
        return content
    else:
        return None


def to_storage(blob_string, bucket_name, prefix, id):
    bucket = storage_client.get_bucket(bucket_name)
    blob_name = f"{prefix}/{id}.archive.gz"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(blob_string)
    logging.info(f"File {blob_name} stored in {bucket_name} successfully!")


def list_bucket(bucket_name, prefix):
    bucket = storage_client.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    file_names = []
    for blob in blobs:
        file_names.append(blob.name)
    logging.info(f"Found a list of {len(file_names)} files in {bucket_name}/{prefix}")
    json_files = [file for file in file_names if '.json' in file]
    return json_files


def delete_old(bucket_name, file_name):
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(file_name)
    blob.delete()


def get_timedelta(iso):
    count = int(iso[:-1])
    if 'Y' in iso:
        return timedelta(days=365*count)
    if 'M' in iso:
        return timedelta(days=30*count)
    if 'D' in iso:
        return timedelta(days=count)
    if 'H' in iso:
        return timedelta(hours=count)
    if 'M' in iso:
        return timedelta(minutes=count)
