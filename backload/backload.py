import gzip
import json
import argparse
from google.cloud import storage
from google.cloud import pubsub_v1

parser = argparse.ArgumentParser()
parser.add_argument("--bucket", "-b", required=True, help="Bucket to get topic history from")
parser.add_argument("--prefix", "-f", help="Prefix of files on bucket to load topic history from")
parser.add_argument("--topic", "-t", required=True, help="Topic to publish to")
parser.add_argument("--project", "-p", required=True, help="Project of topic publish to")
parser.add_argument("--number", "-n", help="Maximum of messages to publish per blob")
parser.add_argument("--preview", "-v", action='store_true', help="Only print messages that would be published")

args = parser.parse_args()

client = storage.Client()
publisher = pubsub_v1.PublisherClient()

print(f"Loading messages from {args.bucket}:/{args.prefix} to {args.project}/topics/{args.topic} (max {args.number} per blob)")

bucket = client.get_bucket(args.bucket)
topic_name = f'projects/{args.project}/topics/{args.topic}'

blobs = client.list_blobs(args.bucket, prefix=args.prefix)

for blob in blobs:
    print(f"Backloading from blob {blob.name}")
    blob_content = gzip.decompress(blob.download_as_string()).decode()
    messages_list = json.loads(blob_content)

    for nr, message in enumerate(messages_list):
        print(f"Message {json.dumps(message)[0:80]}")

        if not args.preview:
            future = publisher.publish(topic_name, json.dumps(message).encode())

        if args.number and nr+1 >= int(args.number):
            break
