import redis


def create_redis_connection(host: str, port: int) -> redis.Redis:
    return redis.Redis(host=host, port=port, db=0)
