import dataclasses
import subprocess
from abc import ABC, abstractmethod
from enum import StrEnum


class Runtimes(StrEnum):
    SUBPROCESS = "subprocess"


@dataclasses.dataclass
class Job:
    command: str
    runtime: Runtimes = Runtimes.SUBPROCESS


class Runtime(ABC):
    @staticmethod
    @abstractmethod
    def run(job: Job) -> None:
        pass


class SubprocessRuntime(Runtime):
    @staticmethod
    def run(job: Job) -> None:
        subprocess.run(job.command, check=True, shell=False)


class Scheduler:
    RUNTIMES = {Runtimes.SUBPROCESS: SubprocessRuntime}

    def __init__(self) -> None:
        self.jobs: list[Job] = []

    def execute(self, job: Job) -> None:
        runtime = self.RUNTIMES[job.runtime]
        runtime.run(job)

    def run(self) -> None:
        while self.jobs:
            job = self.jobs.pop()
            self.execute(job)

    def add(self, job: Job) -> None:
        self.jobs.append(job)
