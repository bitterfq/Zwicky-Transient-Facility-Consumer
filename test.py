from alerce.core import Alerce
client = Alerce()
stamps = client.get_stamps("ZTF18abcjgbw")
print(stamps[0])

import matplotlib.pyplot as plt
import numpy as np

img_data = stamps[0].data

if img_data is not None and img_data.size > 0:
    plt.imshow(img_data, cmap='gray')
    plt.axis('off')
    plt.savefig('ztf_stamp.png', bbox_inches='tight', pad_inches=0.1)
    print("Image saved as ztf_stamp.png")
    plt.show()
else:
    print("No image data found in the FITS stamp.")