import time
import subprocess
import logging
from datetime import datetime

LOG_FILE = 'logs/runner.log'
INTERVAL = 2 * 60 * 60 # 2 hours

# === SETUP LOGGING ===
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s'
)

def run_sync():
    timestamp = datetime.utcnow().isoformat()
    logging.info(f"⏱️ Running sync at {timestamp}")
    subprocess.run(['python3', 'src/sync_to_s3.py'])

if __name__ == "__main__":
    logging.info("📌 Runner started. Will sync every 2 hours.")
    #run_sync()  # Run immediately at startup
    while True:
        time.sleep(INTERVAL)
        run_sync()
