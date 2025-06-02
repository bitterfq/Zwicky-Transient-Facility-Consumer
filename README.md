# ZTF Stream Consumer

This project demonstrates how to consume astronomical alert streams from the Lasair broker using Python, fetch and save ZTF image stamps, and periodically sync data to AWS S3.

## Project Structure

- `src/consumer.py`: Main code to connect to the Lasair Kafka server and consume messages from multiple topics using separate threads.
- `src/fetch_stamps.py`: Fetches and saves ZTF image stamps using the ALeRCE client.
- `src/runner.py`: Schedules and runs periodic syncs to S3.
- `src/sync_to_s3.py`: Handles syncing alert data and images to AWS S3.
- `requirements.txt`: Lists required Python packages.
- `README.md`: Project documentation.
- `logs/`: Stores event, error, and sync logs (not tracked by git).
- `data/`: Stores alert data, partitioned by date (not tracked by git).
- `images/`: Stores fetched image stamps, organized by date (not tracked by git).

## Requirements

- Python 3.x
- See `requirements.txt` for all dependencies (`lasair`, `astropy`, `requests`, `alerce`, `matplotlib`, `boto3`, `schedule`).

## Installation

1. Install dependencies:
    ```sh
    pip install -r requirements.txt
    ```

2. Create necessary directories (if not present):
    ```sh
    mkdir -p logs data images
    ```

## Usage

### Run the consumer script

```sh
python src/consumer.py
```

- Connects to the Lasair Kafka server and starts multiple consumer instances (each in its own thread), listening to different topics.
- Alerts are logged to separate files for each consumer, and both event and error logs are maintained.
- Alert data is partitioned by date, and ZTF image stamps are fetched and saved using the ALeRCE client.

### Periodic S3 Sync

To automatically sync data and images to AWS S3 every 2 hours:

```sh
python src/runner.py
```

## How it works

- Each consumer instance runs in its own thread and listens to a specific Kafka topic.
- Alerts are parsed, Julian Dates are converted to UTC, and all data is logged.
- ZTF image stamps are fetched and saved for each alert.
- The script is designed to run continuously and handle multiple streams at once.
- Data and images are periodically synced to AWS S3 using `boto3`.

## Recent Changes

- Refactored `consumer.py` into a class-based design for multiple concurrent consumers.
- Added threading support to allow multiple consumers to run in parallel.
- Improved error handling and logging.
- Added Julian Date to UTC conversion for alert timestamps.
- Integrated ALeRCE client for fetching and saving ZTF image stamps.
- Added scheduled S3 sync with logging.

## Notes

- You may need access credentials or network permissions to connect to the Lasair Kafka server and AWS S3.
- The `logs/`, `data/`, and `images/` directories are excluded from version control via `.gitignore`.
- For more information about Lasair and astronomical alert streams, visit [https://lasair-ztf.lsst.ac.uk/](https://lasair-ztf.lsst.ac.uk/).