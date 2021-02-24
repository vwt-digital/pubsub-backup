[![CodeFactor](https://www.codefactor.io/repository/github/vwt-digital/pubsub-backup/badge)](https://www.codefactor.io/repository/github/vwt-digital/pubsub-backup)

# Pub/Sub backup

This repository contains multiple functions to backup and process Pub/Sub Topics automatically.

## Functions
The following functions can be found in this repository:
- `mirror-to-azure-pull`: A pull function to automatically mirror certain Pub/Sub Topic towards an Azure EventHub;
- `pubsub-backup`: A function to automatically backup certain Pub/Sub Topic towards a GCS Bucket.
- `push-to-eventhub`: A push function to automatically publish certain Pub/Sub Topic towards an Azure EventHub;


## Scripts
Furthermore, the following scripts can be found in this repository:
- `aggregate`: A script to aggregate topic backup files from a certain day;
- `backload`: A script to backload messages from message history into a topic.

## License
[GPL-3](https://www.gnu.org/licenses/gpl-3.0.en.html)
