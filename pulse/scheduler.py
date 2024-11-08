import dataclasses
import subprocess
from abc import ABC, abstractmethod
from datetime import datetime

import docker
from croniter import croniter

from pulse.constants import DEFAULT_DOCKER_IMAGE, Runtimes
from pulse.logutils import LoggingMixing


@dataclasses.dataclass
class Job:
    command: str | list[str]
    runtime: Runtimes
    schedule: str | None = None
    next_run: datetime | None = None

    def calculate_next_run(self, at: datetime) -> datetime:
        if not self.schedule:
            return at
        cron = croniter(self.schedule, at)
        return cron.get_next(datetime)  # type: ignore[no-any-return]


class Runtime(ABC):
    @abstractmethod
    def run(self, job: Job) -> None:
        pass


class SubprocessRuntime(Runtime, LoggingMixing):
    def run(self, job: Job) -> None:
        self.logger.info("Running command %s", job.command)
        process = subprocess.run(
            job.command,
            check=True,
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for line in process.stdout.splitlines():
            log = line.strip()
            self.logger.info(log)


class DockerRuntime(Runtime, LoggingMixing):
    def __init__(self) -> None:
        super().__init__()
        self.client = docker.from_env()

    def run(self, job: Job) -> None:
        self.logger.info("Running command %s", job.command)
        container = self.client.containers.run(
            DEFAULT_DOCKER_IMAGE, job.command, detach=True
        )
        container.wait()
        logs = container.logs(stream=False)
        decoded = logs.decode("utf-8")
        self.logger.info(decoded.strip())


class Scheduler:
    RUNTIME_CLASSES = {
        Runtimes.SUBPROCESS: SubprocessRuntime,
        Runtimes.DOCKER: DockerRuntime,
    }

    def __init__(self) -> None:
        self._jobs: list[Job] = []

    def _execute(self, job: Job) -> None:
        runtime = self._get_runtime(job)
        runtime.run(job)

    def _get_runtime(self, job: Job) -> Runtime:
        return self.RUNTIME_CLASSES[job.runtime]()

    def run(self) -> None:
        while self._jobs:
            at = datetime.now()
            job = self._jobs.pop(0)
            if not job.next_run:
                job.next_run = job.calculate_next_run(at)

            if at >= job.next_run:
                self._execute(job)
                job.next_run = job.calculate_next_run(job.next_run)

            if job.schedule:
                self._add(job)

    def _add(self, job: Job) -> None:
        self._jobs.append(job)
