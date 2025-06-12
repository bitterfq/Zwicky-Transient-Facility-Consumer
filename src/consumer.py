"""
consumer.py

Kafka consumer for ZTF alert streams using the Lasair broker.
- Consumes alerts from a specified Kafka topic.
- Converts Julian dates to UTC ISO format.
- Writes alerts to date-partitioned JSONL files.
- Downloads and saves associated image cutouts for each alert.
- Logs events and errors for monitoring and debugging.
- Designed for use in automated pipelines and supports multi-threaded consumption.
"""

import json
from datetime import datetime
from lasair import lasair_consumer
from astropy.time import Time
from fetch_stamps import fetch_and_save_all
import threading
import os

class ZTFAlertConsumer:
    def __init__(self, kafka_server, group_id, topic,
                 base_data_dir='data/alerts_partitioned',
                 event_log='logs/event.log',
                 error_log='logs/errors.log'):
        self.kafka_server = kafka_server
        self.group_id = group_id
        self.topic = topic
        self.base_data_dir = base_data_dir
        self.event_log = event_log
        self.error_log = error_log
        self.consumer = lasair_consumer(self.kafka_server, self.group_id, self.topic)
        print(f"[{datetime.utcnow()}] Connected to Kafka topic: {self.topic}")

    def run(self):
        with open(self.event_log, 'a') as evt_file, open(self.error_log, 'a') as err_file:
            while True:
                msg = self.consumer.poll(timeout=1800)
                now = datetime.utcnow().isoformat()

                if msg is None:
                    message = f"[{now}] Poll timeout - no new messages."
                    print(message)
                    evt_file.write(message + '\n')
                    evt_file.flush()
                    continue

                if msg.error():
                    message = f"[{now}] Kafka error: {msg.error()}"
                    print(message)
                    err_file.write(message + '\n')
                    err_file.flush()
                    continue

                try:
                    alert = json.loads(msg.value())

                    for jd_field, utc_field in [
                        ("first_detection", "utc_first_detection"),
                        ("latest_detection", "utc_latest_detection")
                    ]:
                        jd_val = alert.get(jd_field)
                        if jd_val:
                            try:
                                t = Time(jd_val, format='jd')
                                alert[utc_field] = t.iso
                                evt_file.write(f"[{now}] Converted {jd_field} to {utc_field}\n")
                            except Exception as conv_err:
                                alert[utc_field] = None
                                err_file.write(f"[{now}] Failed to convert {jd_field}: {conv_err}\n")

                    object_id = alert.get('objectId', 'unknown')
                    utc_date = alert.get("utc_latest_detection", "")[:10]  # e.g., '2025-05-28'

                    if not utc_date:
                        err_file.write(f"[{now}] Missing utc_latest_detection for {object_id}\n")
                        continue

                    # Write to date-partitioned alerts file
                    output_folder = os.path.join(self.base_data_dir, f"date={utc_date}")
                    os.makedirs(output_folder, exist_ok=True)
                    alert_path = os.path.join(output_folder, "alerts.jsonl")
                    with open(alert_path, 'a') as f:
                        f.write(json.dumps(alert) + '\n')

                    # Save timestamped images in dated folder
                    image_folder = f"images/by_date/{utc_date}"
                    os.makedirs(image_folder, exist_ok=True)
                    fetch_and_save_all(object_id, image_folder)

                    evt_file.write(f"[{now}] Processed alert for objectId: {object_id}\n")
                    evt_file.flush()

                except Exception as e:
                    err_file.write(f"[{now}] JSON parse error or processing failure: {e}\n")
                    err_file.flush()


if __name__ == "__main__":
    '''consumer = ZTFAlertConsumer(
        kafka_server='kafka.lsst.ac.uk:9092',
        group_id='bright_fast_transient_alerts',
        topic='lasair_1568BrightFastTransients'
    )'''

    '''consumer_ft = ZTFAlertConsumer(
        kafka_server='kafka.lsst.ac.uk:9092', 
        group_id='fast_transient_alerts',
        topic='lasair_1568FastTransients',
        output_file='data/alerts_log_ft.jsonl', 
        event_log='logs/event_ft.log', 
        error_log='logs/errors_ft.log'
    )'''

    consumer_loose = ZTFAlertConsumer(
        kafka_server='kafka.lsst.ac.uk:9092', 
        group_id='ztf_loose_repartitioned_v1',
        topic='lasair_1568loosetestfilter',
        event_log='logs/loose/event_loose.log', 
        error_log='logs/loose/errors_loose.log'
    )

    #t1 = threading.Thread(target=consumer.run)
    #t2 = threading.Thread(target=consumer_ft.run)
    t3 = threading.Thread(target=consumer_loose.run)
    
    #t1.start()
    #t2.start()
    t3.start()

    #t1.join()
    #t2.join()
    t3.join()