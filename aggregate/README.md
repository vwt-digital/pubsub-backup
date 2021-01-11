# Aggregate Topic history

This script is an extension to the [Pub/Sub Backup function](../functions/pubsub-backup). It aggregates all files from 
a previous day into a compressed `.xz` file and moves it to the history bucket. This functionality is based on a
data-catalog file containing all Topics.

## Run
The script can be run by executing the following:
```bash
python3 aggregate.py -c data_catalog.json -p [PROJECT_ID]
```

## License
[GPL-3](https://www.gnu.org/licenses/gpl-3.0.en.html)
