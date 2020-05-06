import json
from dataclasses import dataclass
from typing import Dict, Any, Optional

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
        except Exception:
            return self.properties_raw

    def execute(self, job_types: Dict) -> Any:
        if self.job_type not in job_types:
            raise ValueError("Unknown job")

        return job_types[self.job_type](self.properties())


@dataclass_json
@dataclass
class RedisResult:
    status: str
    result: Optional[str]
    response: Optional[Any]
