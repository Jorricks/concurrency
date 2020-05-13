import json
import uuid
import datetime as dt

import redis
from fastapi import FastAPI
from pydantic.main import BaseModel
from pydantic.typing import Dict

from src.concurrier.redis_types import RedisJob, RedisResult


class JobSubmission(BaseModel):
    type: str
    arguments: Dict


def get_app(r: redis.Redis()) -> FastAPI:
    app = FastAPI()

    @app.get("/")
    def read_root():
        return {"Hello": "World"}

    @app.post("/submitjob/")
    def submit_job(*, submission: JobSubmission) -> str:
        job_id = f'job_{uuid.uuid4().hex}'

        r.set(
            job_id,
            RedisResult(
                id=job_id,
                status="queued",
                result=None,
                response=None,
                queue_time=dt.datetime.now(),
                start_time=None,
                end_time=None,
            ).to_json())
        r.rpush('jobs',
                RedisJob(
                    id=job_id,
                    job_type=submission.type,
                    properties_raw=json.dumps(submission.arguments)
                ).to_json())
        return job_id

    @app.get("/status/{job_id}")
    def update_item(job_id: str):
        job = r.get(job_id)
        if job is None:
            return 'Unknown job'

        job_status: RedisResult = RedisResult.from_json(job)

        return job_status.get_dict_with_ms_since_epoch()

    return app
