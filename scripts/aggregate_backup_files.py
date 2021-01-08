import tarfile
import logging
import tempfile
import gzip

from datetime import datetime, timedelta
from google.cloud import storage


class GCSBucketProcessor:
    def __init__(self, client, bucket_name, process_date):
        self.client = client

        self.process_date = process_date
        self.bucket_name = bucket_name
        self.bucket_prefix = datetime.strftime(process_date, '%Y/%m/%d')

        self.aggregated_file_name = f"{bucket_name}_{datetime.strftime(process_date, '%Y%m%d')}.tar.xz"

        self.bucket = self.client.get_bucket(bucket_name)

    def aggregate_blobs(self):
        if not self.bucket:
            raise SystemError(f"Bucket '{self.bucket_name}' does not exist")

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


def aggregate_backup_files():
    stg_client = storage.Client()
    date = datetime.now() - timedelta(2)

    # TODO: List buckets based on ODH data-catalog

    buckets = [bucket.name for bucket in stg_client.list_buckets(fields='items/name') if
               bucket.name.endswith('hst-sa-stg')]

    for name in buckets:
        try:
            GCSBucketProcessor(client=stg_client, bucket_name=name, process_date=date).aggregate_blobs()
        except SystemError as e:
            logging.error(str(e))
            continue
        except Exception as e:
            logging.exception(e)
            continue


aggregate_backup_files()
