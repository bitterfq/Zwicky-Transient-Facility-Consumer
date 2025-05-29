import json
from datetime import datetime
from lasair import lasair_consumer
from astropy.time import Time
from fetch_stamps import fetch_and_save_all
import threading
import os

class ZTFAlertConsumer:
    def __init__(self, kafka_server, group_id, topic,
                 output_file='data/alerts_log.jsonl',
                 event_log='logs/event.log',
                 error_log='logs/errors.log'):
        self.kafka_server = kafka_server
        self.group_id = group_id
        self.topic = topic
        self.output_file = output_file
        self.event_log = event_log
        self.error_log = error_log
        self.consumer = lasair_consumer(self.kafka_server, self.group_id, self.topic)
        print(f"[{datetime.utcnow()}] Connected to Kafka topic: {self.topic}")

    def run(self):
        with open(self.output_file, 'a') as alert_file, \
             open(self.event_log, 'a') as evt_file, \
             open(self.error_log, 'a') as err_file:

            while True:
                msg = self.consumer.poll(timeout=60)
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
                                conversion_msg = f"[{now}] Converted {jd_field} to {utc_field}"
                                print(conversion_msg)
                                evt_file.write(conversion_msg + '\n')
                            except Exception as conv_err:
                                alert[utc_field] = None
                                error_msg = f"[{now}] Failed to convert {jd_field}: {conv_err}"
                                print(error_msg)
                                err_file.write(error_msg + '\n')

                    alert_file.write(json.dumps(alert) + '\n')
                    alert_file.flush()

                    object_id = alert.get('objectId', 'unknown')
                    log_msg = f"[{now}] Logged alert for objectId: {object_id}"
                    print(log_msg)
                    evt_file.write(log_msg + '\n')
                    evt_file.flush()

                    try:
                        fetch_and_save_all(object_id, outdir="images")
                    except Exception as stamp_err:
                        err_file.write(f"[{now}] Stamp fetch error for {object_id}: {stamp_err}\n")
                        err_file.flush()

                except Exception as e:
                    error_msg = f"[{now}] JSON parse error: {e}"
                    print(error_msg)
                    err_file.write(error_msg + '\n')
                    err_file.flush()

if __name__ == "__main__":
    consumer = ZTFAlertConsumer(
        kafka_server='kafka.lsst.ac.uk:9092',
        group_id='bright_fast_transient_alerts',
        topic='lasair_1568BrightFastTransients'
    )

    consumer_ft = ZTFAlertConsumer(
        kafka_server='kafka.lsst.ac.uk:9092', 
        group_id='fast_transient_alerts',
        topic='lasair_1568FastTransients',
        output_file='data/alerts_log_ft.jsonl', 
        event_log='logs/event_ft.log', 
        error_log='logs/errors_ft.log'
    )

    t1 = threading.Thread(target=consumer.run)
    t2 = threading.Thread(target=consumer_ft.run)

    t1.start()
    t2.start()

    t1.join()
    t2.join()