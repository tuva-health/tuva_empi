from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from main.models import Job


@dataclass
class JobResult:
    return_code: int
    error_message: Optional[str]


class JobRunner(ABC):
    @abstractmethod
    def run_job(self, job: Job) -> JobResult:
        pass
