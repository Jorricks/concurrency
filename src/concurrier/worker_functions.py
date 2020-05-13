import os
import urllib.request
from typing import Dict, Union, List, Optional

from PIL import Image


def verify_options(options: Dict, args: List[str]) -> None:
    if not isinstance(options, Dict):
        raise ValueError('Value \'arguments\' was not passed correctly as a json. E.g. {"n":2}.'
                         f'Found value was: {options}')
    for arg in args:
        if arg not in options:
            raise ValueError(f"The argument '{arg}' was not passed as a json key of options.")


def fibonacci(options: Dict) -> Optional[Union[str, int]]:
    verify_options(options, ['n'])
    # options: '{"n": 500000}' takes 2.8 seconds

    n = options['n']
    f_1 = 1
    f_2 = 0
    result = 0
    counter = 1

    while counter <= n:
        result = f_1 + f_2
        f_2 = f_1 if counter > 1 else 0
        f_1 = result
        counter += 1

    return result


def download_images(options: Dict) -> Optional[Union[str, int]]:
    verify_options(options, ['imageUrls', 'folder'])
    image_urls = options['imageUrls']
    folder = options['folder']

    if not os.path.isdir(folder):
        os.makedirs(folder)
    for image_url in image_urls:
        try:
            filename = image_url.split('/')[-1]
            filepath = os.path.join(folder, filename)
            urllib.request.urlretrieve(image_url, filepath)
        except Exception:
            pass
    return True


def convert_image(options: Dict) -> Optional[Union[str, int]]:
    verify_options(options, ['filenames', 'folder'])
    all_filenames = options['filenames']
    folder = options['folder']

    for filename in all_filenames:
        file = os.path.join(folder, filename)
        if not os.path.isfile(file):
            continue

        im = Image.open(file)
        # convert to thumbnail image
        im.thumbnail((128, 128), Image.ANTIALIAS)
        # don't save if thumbnail already exists
        if filename[0:2] != "T_":
            # prefix thumbnail file with T_
            im.save(os.path.join(folder, f"T_{filename}"), "JPEG")
    return True
