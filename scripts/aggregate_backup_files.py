import logging
import argparse
import json
import tempfile
import tarfile
import gzip

from datetime import datetime, timedelta
from google.cloud import storage, exceptions

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--data-catalog', required=True)
parser.add_argument('-p', '--project', required=True)
args = parser.parse_args()
data_catalog = args.data_catalog
project = args.project

stg_client = storage.Client()
date = datetime.now() - timedelta(2)


class GCSBucketProcessor:
    def __init__(self, client, bucket_name_prefix, process_date):
        self.client = client

        self.process_date = process_date
        self.staging_bucket_name = f"{bucket_name_prefix}-hst-sa-stg"
        self.backup_bucket_name = f"{bucket_name_prefix}-history-stg"
        self.bucket_prefix = datetime.strftime(process_date, '%Y/%m/%d')

        self.aggregated_file_name = f"{self.staging_bucket_name}_{datetime.strftime(process_date, '%Y%m%d')}.tar.xz"

        self.bucket = None

    def aggregate_blobs(self):
        try:
            self.bucket = self.client.get_bucket(self.staging_bucket_name)
        except exceptions.NotFound:
            raise SystemError(f"Bucket '{self.staging_bucket_name}' does not exist, skipping aggregation")

        bucket_blobs = list(self.bucket.list_blobs(prefix=self.bucket_prefix))

        if len(bucket_blobs) == 0:
            return

        try:
            temp_file = tempfile.NamedTemporaryFile(mode='w+b', suffix='.tar.xz')

            with tarfile.open(fileobj=temp_file, mode='w:xz') as tar:
                for blob in bucket_blobs:
                    if f"{self.bucket_prefix}/{self.aggregated_file_name}" == blob.name:
                        continue

                    blob_data, blob_size, blob_name = self.get_blob_data(blob)  # Get parsed blob data

                    info = tarfile.TarInfo(blob_name)
                    info.size = blob_size
                    tar.addfile(info, blob_data)
        except Exception as e:
            logging.exception(e)
        else:
            self.save_aggregated_file(temp_file)
            temp_file.close()

            # TODO: Move aggregated file
            # TODO: Delete processed files

    @staticmethod
    def get_blob_data(blob):
        temp_file = tempfile.TemporaryFile(mode='w+b')
        blob.download_to_file(temp_file)
        temp_size = temp_file.tell()
        temp_file.seek(0)

        if blob.name.endswith('.gz'):
            decompressed_file = gzip.GzipFile(fileobj=temp_file, mode='rb')
            dc_temp_file = tempfile.TemporaryFile(mode='w+b')
            dc_temp_file.write(decompressed_file.read())
            dc_temp_size = temp_file.tell()

            dc_temp_file.seek(0)
            temp_file.close()
            return dc_temp_file, dc_temp_size, blob.name.replace('.archive.gz', '.json')

        return temp_file, temp_size, blob.name

    def save_aggregated_file(self, temp_file):
        logging.info(f"Uploading file '{self.aggregated_file_name}' with size '{temp_file.tell()}'")

        blob = self.bucket.blob(f"{self.bucket_prefix}/{self.aggregated_file_name}")
        blob.upload_from_file(temp_file, rewind=True, content_type="application/x-xz")


def get_catalog_topic_names():
    topic_names = []

    catalog_file = open(data_catalog, "r")
    catalog_json = json.load(catalog_file)

    for dataset in catalog_json.get('dataset'):
        for distribution in dataset.get('distribution'):
            if distribution['format'] == 'topic':
                topic_names.append(distribution['title'])

    catalog_file.close()
    return topic_names


def aggregate_backup_files():
    topic_names = get_catalog_topic_names()

    logging.info(f"Found '{len(topic_names)}' topics to aggregate")

    for topic in topic_names:
        try:
            GCSBucketProcessor(client=stg_client, bucket_name_prefix=topic, process_date=date).aggregate_blobs()
        except Exception as e:
            logging.error(str(e))
            continue


aggregate_backup_files()
