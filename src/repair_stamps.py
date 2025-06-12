import json
import os
from pathlib import Path
from fetch_stamps import fetch_and_save_all

alerts_dir = Path("data/alerts_partitioned")
images_dir = Path("images/by_date")

for date_folder in alerts_dir.glob("date=*"):
    alert_date = date_folder.name.split("=")[-1]
    jsonl_path = date_folder / "alerts.jsonl"
    image_folder = images_dir / alert_date
    os.makedirs(image_folder, exist_ok=True)

    if not jsonl_path.exists():
        continue

    with open(jsonl_path) as f:
        for line in f:
            try:
                alert = json.loads(line)
                object_id = alert["objectId"]
                expected = [f"{object_id}_{t}.png" for t in ["science", "template", "difference"]]
                missing = [f for f in expected if not (image_folder / f).exists()]

                if missing:
                    print(f"Re-fetching {object_id} (missing {len(missing)} stamps)")
                    fetch_and_save_all(object_id, image_folder)

            except Exception as e:
                print(f"Error processing alert line: {e}")
