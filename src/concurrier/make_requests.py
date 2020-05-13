import asyncio
import csv
import json
import logging
import os
import webbrowser
from dataclasses import dataclass
from typing import List, Dict

import aiohttp as aiohttp
import plotly.figure_factory as ff
from asgiref.sync import async_to_sync

from src.concurrier.redis_types import RedisResult


@dataclass
class JobSubmission:
    url: str
    payload: Dict


def run_jobs(task: str, host: str, filename: str, batch: int, n: int, html_file: str) -> None:
    """Do distributed of service attack :p

    :param task: Type of task, either download or thumbnail.
    :param host: URL of our webserver
    :param filename: Filename of the CSV with the urls
    :param batch: The number of item that are send in batch format to the HTTP server
    :param n: The number of total jobs we want to submit
    """
    if task != "download" and task != "thumbnail":
        raise ValueError(f"Task type: {task} is unknown")

    submitted_jobs = async_to_sync(queue_jobs)\
        (task=task, host=host, filename=filename, batch=batch, n=n)
    input('Let us wait for the jobs to finish. Press enter when it\'s done.\n')
    statistics = async_to_sync(get_job_stats)\
        (host=host, jobs=submitted_jobs)

    plot_stats(results=statistics, html_file=html_file)
    logging.info('Done. Cya')


async def post_all_jobs(jobs: List[JobSubmission]) -> List[str]:
    async def fetch(session, url, payload):
        async with session.post(url, data=json.dumps(payload)) as response:
            return await response.text()

    conn = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=conn) as session:
        results = await asyncio.gather(
            *[fetch(session, a_job.url, a_job.payload) for a_job in jobs],
            return_exceptions=True
        )
        return results


async def queue_jobs(task: str, host: str, filename: str, batch: int, n: int) -> List[str]:
    """Submits all the jobs (async or sync, depending on batch).

    :param task: Type of task, either download or thumbnail.
    :param host: URL of our webserver
    :param filename: Filename of the CSV with the urls
    :param batch: The number of item that are send in batch format to the HTTP server
    :param n: The number of total jobs we want to submit
    :return: The stats of the jobs.
    """
    folder = 'imgs'
    img_counter = 0
    jobs_to_submit = []

    # Initialise CSV reader
    read_tsv = csv.reader(open(filename), delimiter="\t")
    next(read_tsv)

    while True:
        if img_counter >= n:
            break

        counter = 0
        all_urls = []
        for row in read_tsv:
            all_urls.append(row[0])
            counter += 1
            if counter >= batch:
                break

        if task == "download":
            payload = {
                'type': "DownloadImage",
                'arguments': {"imageUrls": all_urls, "folder": folder}
            }
        elif task == "thumbnail":
            all_files = [url.split('/')[-1] for url in all_urls]
            payload = {
                'type': "ConvertImage",
                'arguments': {"filenames": all_files, "folder": folder}
            }
        else:
            raise ValueError(f"Task type not supported: {task}")

        img_counter += len(all_urls)
        jobs_to_submit.append(JobSubmission(
            url=f'{host}/submitjob/',
            payload=payload
        ))

    list_of_jobs = await post_all_jobs(jobs_to_submit)
    return [job.strip('"') for job in list_of_jobs]


async def fetch_all_jsons(urls):
    async def fetch(session, url):
        async with session.get(url) as response:
            return await response.json()

    conn = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=conn) as session:
        results = await asyncio.gather(*[fetch(session, url) for url in urls], return_exceptions=True)
        return results


async def get_job_stats(host: str, jobs: List[str]) -> List[RedisResult]:
    """Get's all the job stats in an Async matter.

    :param host: URL of our webserver
    :param jobs: All the job_ids of jobs we submitted
    :return: The stats of the jobs.
    """
    urls = [f'{host}/status/{job_id.strip("")}' for job_id in jobs]
    all_jsons = await fetch_all_jsons(urls)
    return [RedisResult.convert_ms_to_redisresult(result) for result in all_jsons]


def plot_stats(results: List[RedisResult], html_file: str) -> None:
    """Plots the stats by creating a HTML page and opens this HTML page directly in a new tab.

    :param results: The results from the queueing
    :param html_file: The final html_file to be created
    """
    folder = 'output'

    min_queue = min([r.queue_time for r in results])
    max_finish = max([r.end_time for r in results])
    d = [dict(Task='Full running time', Start=min_queue, Finish=max_finish)]

    if not os.path.isdir(folder):
        os.makedirs(folder)

    for specific in ['full', 'running']:
        df = d + [
            dict(
                Task=r.id,
                Start=r.queue_time if specific == 'full' else r.start_time,
                Finish=r.end_time
            )
            for r in results
        ]
        output_file = f'{folder}/{html_file}_{specific}.html'
        fig = ff.create_gantt(df)
        fig.update_layout(title=f"Plot of {html_file}_{specific}.")
        fig.write_html(output_file)
        webbrowser.open(f'file://{os.path.abspath(output_file)}', new=2)

