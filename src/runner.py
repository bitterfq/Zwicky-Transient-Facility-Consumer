from sync_to_s3 import main as sync_main
import schedule
import time
import logging
from datetime import datetime

log_file = "logs/sync_runner.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S'
)

logging.info("Scheduler started, will run every 2 hours.")

# Force immediate first run
logging.info("Running initial sync immediately for sanity check.")
sync_main()

schedule.every(2).hours.do(sync_main)

while True:
    schedule.run_pending()
    time.sleep(60)
