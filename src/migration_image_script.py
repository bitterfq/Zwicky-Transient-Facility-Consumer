import os
import re
from pathlib import Path
from collections import defaultdict

base_dir = Path("images/by_date")

# Regex to extract objectId and image type from filenames
pattern = re.compile(r"(ZTF\w+)_\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.\d{3}_(science|template|difference)\.(png|fits|jpg|tiff)$")

seen = defaultdict(set)  # (objectId, type) seen per date

for date_dir in base_dir.iterdir():
    if not date_dir.is_dir():
        continue

    for file in sorted(date_dir.iterdir()):
        match = pattern.match(file.name)
        if not match:
            continue

        object_id, image_type, ext = match.groups()
        key = (object_id, image_type)

        if key in seen[date_dir.name]:
            print(f"Skipping duplicate: {file}")
            file.unlink()  # Optional: delete duplicates
            continue

        new_name = f"{object_id}_{image_type}.{ext}"
        new_path = date_dir / new_name

        if new_path.exists():
            print(f"Already exists: {new_path}, skipping rename")
            continue

        file.rename(new_path)
        seen[date_dir.name].add(key)
        print(f"Renamed: {file.name} → {new_name}")
