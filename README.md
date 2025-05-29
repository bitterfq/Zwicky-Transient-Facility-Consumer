# ZTF Stream Consumer

This project demonstrates how to consume astronomical alert streams from the Lasair broker using Python.

## Project Structure

- `consumer.py`: Main code to connect to the Lasair Kafka server and consume messages from multiple topics using separate threads.
- `requirements.txt`: Lists required Python packages (`lasair`, `astropy`).
- `README.md`: Project documentation.

## Requirements

- Python 3.x
- The `lasair` and `astropy` Python packages (see `requirements.txt`).

## Installation

1. Install dependencies:
    ```sh
    pip install -r requirements.txt
    ```

## Usage

Run the consumer script:
```sh
python consumer.py
```

The script will connect to the Lasair Kafka server and start multiple consumer instances (each in its own thread), listening to different topics. Alerts are logged to separate files for each consumer, and both event and error logs are maintained.

## How it works

- Each consumer instance runs in its own thread and listens to a specific Kafka topic.
- Alerts are parsed, Julian Dates are converted to UTC, and all data is logged.
- The script is designed to run continuously and handle multiple streams at once.

## Recent Changes

- Refactored `consumer.py` into a class-based design for multiple concurrent consumers.
- Added threading support to allow multiple consumers to run in parallel.
- Improved error handling and logging.
- Added Julian Date to UTC conversion for alert timestamps.

## Notes

- You may need access credentials or network permissions to connect to the Lasair Kafka server.
- For more information about Lasair and astronomical alert streams, visit [https://lasair-ztf.lsst.ac.uk/](https://lasair-ztf.lsst.ac.uk/).