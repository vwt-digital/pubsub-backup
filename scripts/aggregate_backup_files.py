import logging
import argparse
import json
import tempfile
import tarfile
import gzip

from datetime import datetime, timedelta
from google.cloud import storage, exceptions as gcp_exceptions
from retry import retry

logging.getLogger().setLevel(logging.INFO)

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
        """
        Initializes GCS Bucket processor.
        """

        self.client = client

        self.process_date = process_date
        self.bucket_name_prefix = bucket_name_prefix
        self.staging_bucket_name = f"{bucket_name_prefix}-hst-sa-stg"
        self.backup_bucket_name = f"{bucket_name_prefix}-history-stg"
        self.bucket_prefix = datetime.strftime(process_date, '%Y/%m/%d')

        self.aggregated_file_name = f"{self.staging_bucket_name}_{datetime.strftime(process_date, '%Y%m%d')}.tar.xz"

        self.staging_bucket = None
        self.backup_bucket = None

    def aggregate_blobs(self):
        """
        Aggregates blobs within class bucket.
        """

        try:  # Connect to staging (hst-sa-stg) and backup (history-stg) buckets
            self.staging_bucket = self.client.get_bucket(self.staging_bucket_name)
            self.backup_bucket = self.client.get_bucket(self.backup_bucket_name)
        except gcp_exceptions.NotFound as e:
            raise SystemError(f"{str(e)}, skipping aggregation")

        bucket_blobs = list(self.staging_bucket.list_blobs(prefix=self.bucket_prefix))

        if len(bucket_blobs) == 0:
            return

        try:
            temp_file = tempfile.NamedTemporaryFile(mode='w+b', suffix='.tar.xz')

            with tarfile.open(fileobj=temp_file, mode='w:xz') as tar:
                for blob in bucket_blobs:
                    # Skip possible previous created aggregation file
                    if f"{self.bucket_prefix}/{self.aggregated_file_name}" == blob.name:
                        continue

                    # Get parsed blob data
                    blob_data, blob_size, blob_name = self.get_blob_data(blob)

                    # Add file and info to tar
                    info = tarfile.TarInfo(blob_name)
                    info.size = blob_size
                    tar.addfile(info, blob_data)
        except Exception as e:
            logging.exception(e)
        else:
            self.process_aggregated_file(temp_file)

    @staticmethod
    @retry(tries=3, delay=2, backoff=2)
    def get_blob_data(blob):
        """
        Retrieves blob data from GCS bucket, does parse .gz files first.
        """

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

    def process_aggregated_file(self, temp_file):
        """
        Processes created aggregated file towards correct buckets.
        """

        try:
            staging_blob = self.save_aggregated_file(temp_file)
            history_file_exists = self.move_aggregated_file(staging_blob)
        except Exception:
            raise
        else:
            # TODO: Delete processed files

            return history_file_exists

    @retry(tries=3, delay=2, backoff=2)
    def save_aggregated_file(self, temp_file):
        """
        Saves aggregated file towards staging bucket (-hst-sa-stg).
        """

        blob_name = f"{self.bucket_prefix}/{self.aggregated_file_name}"
        blob_location = f"gs://{self.staging_bucket_name}/{blob_name}"
        logging.info(f"Uploading aggregated file to '{blob_location}'")

        try:
            blob = self.staging_bucket.blob(blob_name)
            blob.upload_from_file(temp_file, rewind=True, content_type="application/x-xz")
        except Exception as e:
            logging.error(f"Something went wrong during file upload: {str(e)}")
            raise
        else:
            temp_file.close()
            return blob

    @retry(tries=3, delay=2, backoff=2)
    def move_aggregated_file(self, staging_blob):
        """
        Copies aggregated file towards history bucket (-history-stg).
        """

        blob_name = f"{self.bucket_prefix}/{self.aggregated_file_name}"
        blob_location = f"gs://{self.backup_bucket_name}/{blob_name}"
        logging.info(f"Copying aggregated file to '{blob_location}'")

        try:
            self.staging_bucket.copy(staging_blob, self.backup_bucket, new_name=blob_name)
        except Exception as e:
            logging.error(f"Something went wrong during file copy: {str(e)}")
            raise
        else:
            return storage.Blob(bucket=self.backup_bucket, name=blob_name).exists(self.client)


def get_catalog_topic_names():
    """
    Retrieves Pub/Sub Topic names from passed data-catalog.
    """

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
    """
    Retrieves list of topics and aggregates all blobs.
    """

    topic_names = get_catalog_topic_names()

    logging.info(f"Found '{len(topic_names)}' topics to aggregate")

    for topic in topic_names:
        try:
            logging.info(f"Starting aggregation for topic '{topic}'")
            GCSBucketProcessor(client=stg_client, bucket_name_prefix=topic, process_date=date).aggregate_blobs()
        except Exception as e:
            logging.error(f"Failed aggregation for topic '{topic}': {str(e)}")
            continue
        else:
            logging.info(f"Finished aggregation for topic '{topic}'")


aggregate_backup_files()
