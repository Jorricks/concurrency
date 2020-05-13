import os
import urllib.request
from typing import Dict, Union, List, Optional

from PIL import Image


download_dir = 'imgs'


def verify_options(options: Dict, args: List[str]) -> None:
    if not isinstance(options, Dict):
        raise ValueError("Value 'arguments' was not passed correctly as a json. E.g. {\"n\":2}.")
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


def download_image(options: Dict) -> Optional[Union[str, int]]:
    verify_options(options, ['url', 'filename'])

    if not os.path.isdir(download_dir):
        os.makedirs(download_dir)

    urllib.request.urlretrieve(options['url'], os.path.join(download_dir, options['filename']))
    return True


def convert_image(options: Dict) -> Optional[Union[str, int]]:
    verify_options(options, ['filename'])

    file = os.path.join(download_dir, options['filename'])

    im = Image.open(file)
    # convert to thumbnail image
    im.thumbnail((128, 128), Image.ANTIALIAS)
    # don't save if thumbnail already exists
    if options['filename'][0:2] != "T_":
        # prefix thumbnail file with T_
        im.save(os.path.join(download_dir, f"T_{options['filename']}"), "JPEG")
    return True
