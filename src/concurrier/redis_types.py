import json
from dataclasses import dataclass
from typing import Dict, Any, Optional
import datetime as dt

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class RedisJob:
    id: str
    job_type: str
    properties_raw: str

    def properties(self):
        try:
            return json.loads(self.properties_raw)
        except Exception as exc:
            raise ValueError(f'Failed to load json: {self.properties_raw}')

    def execute(self, job_types: Dict) -> Any:
        if self.job_type not in job_types:
            raise ValueError(f"Job type: {self.job_type} does not exist.")

        return job_types[self.job_type](self.properties())


@dataclass_json
@dataclass
class RedisResult:
    id: str
    status: str
    result: Optional[str]
    response: Optional[Any]
    queue_time: dt.datetime
    start_time: Optional[dt.datetime]
    end_time: Optional[dt.datetime]

    @staticmethod
    def convert_to_ms(time: Optional[dt.datetime]) -> Optional[float]:
        if time is None:
            return None
        return time.timestamp()

    @staticmethod
    def convert_ms_to_dt(time: Optional[float]) -> Optional[dt.datetime]:
        if time is None:
            return None
        return dt.datetime.fromtimestamp(time)

    def get_dict_with_ms_since_epoch(self) -> Dict:
        d = self.to_dict()
        d['queue_time'] = self.convert_to_ms(self.queue_time)
        d['start_time'] = self.convert_to_ms(self.start_time)
        d['end_time'] = self.convert_to_ms(self.end_time)
        return d

    @staticmethod
    def convert_ms_to_redisresult(d) -> "RedisResult":
        d['queue_time'] = RedisResult.convert_ms_to_dt(d['queue_time'])
        d['start_time'] = RedisResult.convert_ms_to_dt(d['start_time'])
        d['end_time'] = RedisResult.convert_ms_to_dt(d['end_time'])
        return RedisResult.from_dict(d)
