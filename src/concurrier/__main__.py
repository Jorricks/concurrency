import logging
import sys
from typing import Optional, Sequence

import click

from src.concurrier.make_requests import run_jobs
from src.concurrier.util import create_redis_connection
from src.concurrier.worker import start_worker
from .restapi import get_app

root = logging.getLogger()
root.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)


@click.group()
@click.version_option()
def main() -> None:
    """
    Command line interface for the demand side platform.
    Provides commands for fitting model
    """


@main.command()
@click.option("-h", "--host-redis", default="localhost")
@click.option("-p", "--port-redis", default=6379)
def serve(host_redis: str, port_redis: int) -> Optional[int]:
    r = create_redis_connection(host_redis, port_redis)
    import uvicorn
    return uvicorn.run(get_app(r), host="localhost", port=8001)


@main.command()
@click.option("-h", "--host-redis", default="localhost")
@click.option("-p", "--port-redis", default=6379)
def worker(host_redis: str, port_redis: int) -> None:
    r = create_redis_connection(host_redis, port_redis)
    start_worker(r)


@main.command()
@click.option("-t", "--task", type=click.Choice(['download', 'thumbnail'], case_sensitive=False))
@click.option("-u", "--url", default="http://localhost:8001")
@click.option("-f", "--filename", default="open-images-dataset-train2.tsv")
@click.option("-b", "--batch", default=1, type=int)
@click.option("-n", "--no-jobs", default=1000, type=int)
def dos(task: str, url: str, filename: str, batch: int, no_jobs: int) -> None:
    return run_jobs(task=task, host=url, filename=filename, batch=batch, n=no_jobs)


if __name__ == "__main__":
    sys.exit(main(*sys.argv))
