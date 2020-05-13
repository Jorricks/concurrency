import threading
import time
from typing import Dict, Callable, Optional, Union, Any

import redis

import src.concurrier.worker_functions as wf
from src.concurrier.redis_types import RedisJob, RedisResult
import logging
import datetime as dt


def start_worker(r: redis.Redis, executor: str, number_of_threads):
    logging.info(f'Starting up {executor} worker')
    job_types = {
        'Fibonacci': wf.fibonacci,
        'DownloadImage': wf.download_images,
        'ConvertImage': wf.convert_image,
    }
    logging.info(f'Worker registered the following job types: {list(job_types.keys())}')
    if executor == 'sequential':
        start_sequential_worker(r, job_types)
    elif executor == 'threaded':
        start_threaded_worker(r, job_types, number_of_threads)
    else:
        raise NotImplementedError(f'Worker {executor} not implemented.')


def update_redis_result(
    r: redis.Redis,
    job_id: str,
    status: str,
    result: Optional[str] = None,
    response: Optional[Any] = None,
    end: bool = True
) -> RedisResult:
    try:
        redis_result: RedisResult = RedisResult.from_json(r.get(job_id))
    except TypeError:
        raise ValueError(f'Job with job_id: {job_id} doesn\'t exist yet')
    redis_result.status = status
    redis_result.result = result
    redis_result.response = response
    redis_result.start_time = dt.datetime.now() if not end else redis_result.start_time
    redis_result.end_time = dt.datetime.now() if end else None
    r.set(job_id, redis_result.to_json())
    return redis_result


def start_sequential_worker(
    r: redis.Redis,
    job_types: Dict[str, Callable[[Dict], Optional[Union[str, int]]]]
) -> None:
    while True:
        redis_job: RedisJob = RedisJob.from_json(r.blpop('jobs')[1])
        job_id = redis_job.id

        logging.info(f'Picked up {job_id}')
        update_redis_result(r, job_id, status='Picked up', end=False)

        try:
            start_time = time.time()
            rj_result = redis_job.execute(job_types)

            update_redis_result(r, job_id, status='Finished', result='Success', response=rj_result)

            logging.info(
                f'Successfully finished {redis_job.id}\tResult: {rj_result}'
                f'\tTime: {time.time() - start_time} seconds'
            )

        except Exception as exc:
            update_redis_result(r, job_id, status='Finished', result='Failed', response=str(exc))

            logging.error(
                f'Failed to run {redis_job.id}. Exception: '
                f'{str(exc)}'
            )


def start_threaded_worker(
    r: redis.Redis,
    job_types: Dict[str, Callable[[Dict], Optional[Union[str, int]]]],
    number_of_threads: int
) -> None:
    for _ in range(number_of_threads):
        threading.Thread(target=start_sequential_worker, args=(r, job_types)).start()
