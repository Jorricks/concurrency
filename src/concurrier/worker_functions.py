from typing import Dict, Union


def fibonacci(options: Dict) -> Union[str, int]:
    # options: '{"n": 500000}' takes 2.8 seconds
    if not isinstance(options, Dict):
        raise ValueError("Value 'arguments' was not passed correctly as a json. E.g. {\"n\":2}.")
    if 'n' not in options:
        raise ValueError("The argument 'n' was not passed as a json key of options.")

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
