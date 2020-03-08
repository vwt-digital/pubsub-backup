# Pubsub topic backload

This repository contains a Python program to backload messages from message history into a topic. The message history is stored on a bucket in the format as written by [pubsub_backup](https://github.com/vwt-digital/pubsub-backup).

```
usage: backload.py [-h] [--bucket BUCKET] [--prefix PREFIX] [--topic TOPIC]
                   [--project PROJECT] [--number NUMBER] [--preview]

optional arguments:
  -h, --help            show this help message and exit
  --bucket BUCKET, -b BUCKET
                        Bucket to get topic history from
  --prefix PREFIX, -f PREFIX
                        Prefix of files on bucket to load topic history from
  --topic TOPIC, -t TOPIC
                        Topic to publish to
  --project PROJECT, -p PROJECT
                        Project of topic publish to
  --number NUMBER, -n NUMBER
                        Maximum of messages to publish per blob
  --preview, -v         Only print messages that would be published

```
