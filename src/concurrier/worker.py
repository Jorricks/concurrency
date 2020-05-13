import time

import redis

import src.concurrier.worker_functions as wf
from src.concurrier.redis_types import RedisJob, RedisResult
import logging


def start_worker(r: redis.Redis):
    logging.info('Starting up worker')
    job_types = {
        'Fibonacci': wf.fibonacci,
        'DownloadImage': wf.download_image,
        'ConvertImage': wf.convert_image,
    }
    logging.info(f'Worker registered the following job types:\n {list(job_types.keys())}')

    while True:
        redis_job: RedisJob = RedisJob.from_json(r.blpop('jobs')[1])

        logging.info(f'Picked up {redis_job.id}')
        r.set(redis_job.id, RedisResult('Picked up', None, None).to_json())

        try:
            start_time = time.time()
            rj_result = redis_job.execute(job_types)
            r.set(redis_job.id, RedisResult('Finished', "Success", rj_result).to_json())

            logging.info(
                f'Successfully finished {redis_job.id}\n'
                f'Result: {rj_result}\n'
                f'Time: {time.time() - start_time} seconds\n')

        except Exception as exc:
            r.set(redis_job.id, RedisResult('Finished', "Failed", str(exc)).to_json())

            logging.info(f'Failed to run {redis_job.id}')
            logging.error(f'Exception: {str(exc)}')
