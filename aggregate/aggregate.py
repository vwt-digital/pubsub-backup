import logging
import argparse
import json
import tempfile
import tarfile
import zlib

from datetime import datetime, timedelta
from google import api_core
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
date = datetime.now() - timedelta(1)


class GCSBucketProcessor:
    def __init__(self, client, bucket_name_prefix, process_date):
        """
        Initializes GCS Bucket processor.
        """

        self.client = client

        self.process_date = process_date
        self.current_timestamp = int(datetime.utcnow().strftime('%s'))

        self.bucket_name_prefix = bucket_name_prefix
        self.staging_bucket_name = f"{bucket_name_prefix}-hst-sa-stg"
        self.backup_bucket_name = f"{bucket_name_prefix}-history-stg"
        self.bucket_prefix = datetime.strftime(process_date, '%Y/%m/%d')

        self.aggregated_file_name_prefix = f"{self.staging_bucket_name}_{datetime.strftime(process_date, '%Y%m%d')}"
        self.aggregated_json_name = f"{self.aggregated_file_name_prefix}_{self.current_timestamp}.json"
        self.aggregated_blob_name = f"{self.bucket_prefix}/{self.aggregated_file_name_prefix}_{self.current_timestamp}.tar.xz"

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
        bucket_blobs_len = len(bucket_blobs)
        bucket_blobs_processed = []
        bucket_blobs_data_json = []
        cur_blob = 0

        if bucket_blobs_len == 0:
            logging.info('Found no backup files')
            return

        temp_tar_file = tempfile.NamedTemporaryFile(mode='w+b', suffix='.tar.xz')

        with tarfile.open(fileobj=temp_tar_file, mode='w:xz') as tar:
            for blob in bucket_blobs:
                try:
                    cur_blob += 1
                    cur_temp_loc = temp_tar_file.tell()

                    # Skip possible previous created aggregation file
                    if self.aggregated_file_name_prefix in blob.name:
                        logging.info(f"Skipping... {cur_blob}/{bucket_blobs_len}")
                        continue

                    # Get parsed blob data
                    blob_data, blob_name = self.get_blob_data(blob)

                    try:
                        blob_data_dc = blob_data.decode('utf-8')
                        blob_data_json = json.loads(blob_data_dc)
                    except (AttributeError, UnicodeDecodeError) as e:
                        if blob_data:  # Add blob to tar
                            logging.info(f"Aggregating... {cur_blob}/{bucket_blobs_len} ({blob.name})")

                            self.add_blob_to_tar(tar, blob_data, blob_name)
                            bucket_blobs_processed.append(blob.name)
                        else:
                            logging.error(f"Skipping... {cur_blob}/{bucket_blobs_len} ({blob.name}): {str(e)}")
                        continue
                    else:
                        logging.info(f"Extending... {cur_blob}/{bucket_blobs_len}")

                        bucket_blobs_data_json.extend(blob_data_json)
                        bucket_blobs_processed.append(blob.name)
                except Exception as e:
                    logging.error(f"Skipping... {cur_blob}/{bucket_blobs_len}: {str(e)}")
                    temp_tar_file.seek(cur_temp_loc)

                    if blob.name in bucket_blobs_processed:
                        del bucket_blobs_processed[blob.name]

                    continue

            # Add JSON data to tar file
            if len(bucket_blobs_data_json) > 0:
                logging.info(f"Aggregating... json/{bucket_blobs_len} ({self.aggregated_json_name})")
                self.add_blob_to_tar(
                    tar, json.dumps(bucket_blobs_data_json).encode('utf-8'), self.aggregated_json_name)

        self.process_aggregated_file(temp_tar_file, bucket_blobs_processed)

    def add_blob_to_tar(self, tar_file, blob_data, blob_name):
        with tempfile.TemporaryFile(mode='w+b') as data_temp_file:
            data_temp_file.write(blob_data)

            info = tarfile.TarInfo(blob_name.split('/')[-1])
            info.size = data_temp_file.tell()
            info.mtime = self.current_timestamp

            data_temp_file.seek(0)
            tar_file.addfile(info, data_temp_file)

    @staticmethod
    @retry(tries=3, delay=2, backoff=2)
    def get_blob_data(blob):
        """
        Retrieves blob data from GCS bucket, does parse .gz files first.
        """

        blob_string = blob.download_as_string()
        blob_name = blob.name

        if blob.name.endswith('.archive.gz'):
            blob_string = zlib.decompress(blob_string, 16 + zlib.MAX_WBITS)
            blob_name = blob_name.replace('.archive.gz', '.json')

        return blob_string, blob_name

    def process_aggregated_file(self, temp_file, processed_blobs):
        """
        Processes created aggregated file towards correct buckets.
        """

        try:
            staging_blob = self.save_aggregated_file(temp_file)
            self.move_aggregated_file(staging_blob)
        except Exception:
            raise
        else:
            processed_blobs.append(staging_blob.name)
            self.delete_obsolete_blobs(processed_blobs)

    @retry(tries=3, delay=2, backoff=2)
    def save_aggregated_file(self, temp_file):
        """
        Saves aggregated file towards staging bucket (-hst-sa-stg).
        """

        blob_location = f"gs://{self.staging_bucket_name}/{self.aggregated_blob_name}"
        logging.info(f"Uploading aggregated file to '{blob_location}'")

        try:
            blob = self.staging_bucket.blob(self.aggregated_blob_name)
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

        blob_location = f"gs://{self.backup_bucket_name}/{self.aggregated_blob_name}"
        logging.info(f"Copying aggregated file to '{blob_location}'")

        try:
            self.staging_bucket.copy_blob(staging_blob, self.backup_bucket, new_name=self.aggregated_blob_name)
        except Exception as e:
            logging.error(f"Something went wrong during file copy: {str(e)}")
            raise
        else:
            if not storage.Blob(bucket=self.backup_bucket, name=self.aggregated_blob_name).exists(self.client):
                raise FileNotFoundError(f"Blob '{blob_location}' does not exist")

    def delete_obsolete_blobs(self, blobs):
        """
        Deletes a list of blobs from the staging bucket (-hst-sa-stg).
        """

        logging.info(f"Deleting {len(blobs)} obsolete files")
        for blob_name in blobs:
            try:
                self.raise_blob_event_hold(blob_name)
            except Exception as e:
                logging.error(f"Failed to raise event-based hold of blob '{blob_name}', skipping deletion: {str(e)}")
                continue

            try:
                retry_policy = api_core.retry.Retry(deadline=60)
                self.staging_bucket.delete_blob(blob_name, retry=retry_policy)
            except Exception as e:
                logging.error(f"Failed to delete blob '{blob_name}', resetting event-based hold: {str(e)}")

                self.reset_blob_event_hold(blob_name)
                continue

    @retry(tries=3, delay=2, backoff=2)
    def raise_blob_event_hold(self, blob_name):
        blob = self.staging_bucket.blob(blob_name)

        blob.event_based_hold = False
        blob.patch()

    @retry(tries=3, delay=2, backoff=2)
    def reset_blob_event_hold(self, blob_name):
        blob = self.staging_bucket.blob(blob_name)

        blob.event_based_hold = True
        blob.patch()


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

    logging.info(f"Found {len(topic_names)} topics to aggregate")

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
