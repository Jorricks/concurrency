import csv
from typing import List

import aiohttp as aiohttp
import httpx

from src.concurrier.redis_types import RedisResult


def load_file(filename: str) -> None:
    read_tsv = csv.reader(open(filename), delimiter="\t")
    for row in read_tsv:
        yield row


def run_jobs(task: str, host: str, filename: str, batch: int, n: int) -> None:
    """Do distributed of service attack :p

    :param task: Type of task, either download or thumbnail.
    :param host: URL of our webserver
    :param filename: Filename of the CSV with the urls
    :param batch: The number of item that are send in batch format to the HTTP server
    :param n: The number of total jobs we want to submit
    """
    if task != "download" and task != "thumbnail":
        raise ValueError(f"Task type: {task} is unknown")
    submitted_jobs = queue_jobs(task=task, host=host, filename=filename, batch=batch, n=n)
    statistics = get_job_stats(url=host, jobs=submitted_jobs)
    # @ToDo do something with stats
    print('Do something with stats')


def queue_jobs(task: str, host: str, filename: str, batch: int, n: int) -> List[str]:
    """Submits all the jobs (async or sync, depending on batch).

    :param task: Type of task, either download or thumbnail.
    :param host: URL of our webserver
    :param filename: Filename of the CSV with the urls
    :param batch: The number of item that are send in batch format to the HTTP server
    :param n: The number of total jobs we want to submit
    :return: The stats of the jobs.
    """
    read_tsv = csv.reader(open(filename), delimiter="\t")
    list_of_jobs = []

    while True:
        if len(list_of_jobs) > n:
            break

        counter = 0
        collection = {}
        for row in read_tsv:
            counter += 1
            if counter > batch:
                break

            url = row[0]
            filename = url.split('/')[-1]

            collection[filename] = url

        if batch > 1:
            raise NotImplementedError('Fix this')
        else:
            for url, filename in collection.items():
                r = httpx.post(f'{host}/submitjob/', json={"url": url, "filename": filename})
                print(r.text)
                if r.status_code != 200:
                    raise ValueError(f'Status code was incorrect: {r.status_code}')

    return list_of_jobs


def get_job_stats(url: str, jobs: List[str]) -> List[RedisResult]:
    """Get's all the job stats in an Async matter.

    :param url: URL of our webserver
    :param jobs: All the job_ids of jobs we submitted
    :return: The stats of the jobs.
    """
    conn = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=conn) as client:
        all_requests = [client.get(f'{url}/status/{job_id}') for job_id in jobs]
        all_requests = [await job_request for job_request in all_requests]
        return [RedisResult.from_json(result) for result in all_requests]
