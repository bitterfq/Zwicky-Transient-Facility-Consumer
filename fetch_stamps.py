"""Fetch and save ZTF stamps using Alerce client."""
from alerce.core import Alerce
import matplotlib.pyplot as plt
from astropy.io import fits
import os

def fetch_stamps(ztf_id):
    """Fetch all stamps for a given ZTF ID."""
    client = Alerce()
    stamps = client.get_stamps(ztf_id)
    if stamps and len(stamps) >= 3:
        return stamps  # List of 3 PrimaryHDU objects
    else:
        raise ValueError(f"Less than 3 stamps found for ZTF ID: {ztf_id}")

def save_stamp_png(stamp, filename):
    """Save a FITS stamp as a PNG image."""
    img_data = stamp.data
    if img_data is not None and img_data.size > 0:
        plt.imshow(img_data, cmap='gray')
        plt.axis('off')
        plt.savefig(filename, bbox_inches='tight', pad_inches=0.1)
        plt.close()
        print(f"Image saved as {filename}")
    else:
        print(f"No image data found in the FITS stamp for {filename}.")

def fetch_and_save_all(ztf_id, outdir="."):
    """Fetch stamps for a ZTF ID and save all as PNG and FITS."""
    try:
        stamps = fetch_stamps(ztf_id)
        names = ["science", "template", "difference"]
        for i, (stamp, name) in enumerate(zip(stamps, names)):
            png_path = os.path.join(outdir, f"{ztf_id}_{name}.png")
            save_stamp_png(stamp, png_path)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python fetch_stamps.py <ZTF_OBJECT_ID> [output_directory]")
    else:
        ztf_id = sys.argv[1]
        outdir = sys.argv[2] if len(sys.argv) > 2 else "."
        fetch_and_save_all(ztf_id, outdir)