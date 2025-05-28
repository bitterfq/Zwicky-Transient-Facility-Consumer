import json
from datetime import datetime
from lasair import lasair_consumer
from astropy.time import Time

kafka_server = 'kafka.lsst.ac.uk:9092'
group_id = 'bright_fast_alerts'
my_topic = 'lasair_1568BrightFastTransients'

output_file = 'alerts_log.jsonl'
event_log = 'event.log'
error_log = 'errors.log'

consumer = lasair_consumer(kafka_server, group_id, my_topic)
print(f"[{datetime.utcnow()}] Connected to Kafka topic: {my_topic}")

with open(output_file, 'a') as alert_file, \
     open(event_log, 'a') as evt_file, \
     open(error_log, 'a') as err_file:

    while True:
        msg = consumer.poll(timeout=60)
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

        except Exception as e:
            error_msg = f"[{now}] JSON parse error: {e}"
            print(error_msg)
            err_file.write(error_msg + '\n')
            err_file.flush()
