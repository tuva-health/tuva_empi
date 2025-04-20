from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class JobResult:
    return_code: int
    error_message: Optional[str]


class JobRunner(ABC):
    @abstractmethod
    def run_job(self) -> JobResult:
        pass
