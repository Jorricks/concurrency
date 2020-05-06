from .restapi import get_app
from .util import create_redis_connection

app = get_app(create_redis_connection("localhost", 6379))

