import uuid

import redis
from fastapi import FastAPI

from src.concurrier.redis_types import RedisJob, RedisResult


def get_app(r: redis.Redis()) -> FastAPI:
    app = FastAPI()

    @app.get("/")
    def read_root():
        return {"Hello": "World"}

    @app.put("/submitjob/")
    def submit_job(type: str, arguments: str) -> str:
        job_id = f'job_{uuid.uuid4().hex}'
        r.rpush('jobs', RedisJob(job_id, type, arguments).to_json())
        r.set(job_id, RedisResult("queued", None, None).to_json())
        return job_id

    @app.get("/status/{job_id}")
    def update_item(job_id: str):
        job = r.get(job_id)
        if job is None:
            return 'Unknown job'

        job_status: RedisResult = RedisResult.from_json(job)
        return {
            "job_status": job_status.status,
            "job_result": job_status.result,
            "job_response": job_status.response,
        }

    return app
