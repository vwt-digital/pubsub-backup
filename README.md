# Pubsub backup

This repository contains functions to backup and compress pubsub messages. In an event-driven architecture, it may be useful to store the event history that passes a topic to a storage bucket. This facilitates the replaying of the entire event history to, for example, another topic.

![pubsub backup](pubsub-backup.svg)

Note: topics, push subscriptions and buckets not included!

Literature: [Pub/sub push subscriptions](https://cloud.google.com/pubsub/docs/push)
