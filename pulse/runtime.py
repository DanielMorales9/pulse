import subprocess
from abc import ABC, abstractmethod

import docker

from pulse.constants import DEFAULT_DOCKER_IMAGE, RuntimeType
from pulse.logutils import LoggingMixing
from pulse.models import Job


class Runtime(ABC):
    @staticmethod
    def render_command(job: Job) -> str:
        return job.command.format(
            execution_time=job.execution_time,
            to_date=job.next_run,
            from_date=job.prev_run,
            id=job.id,
        )

    @abstractmethod
    def run(self, job: Job) -> None:
        pass


class SubprocessRuntime(Runtime, LoggingMixing):
    def run(self, job: Job) -> None:
        self.logger.debug("Running command %s", job.command)
        process = subprocess.run(
            self.render_command(job).split(" ", 1),
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
        self.logger.debug("Running command %s", job.command)
        container = self.client.containers.run(
            DEFAULT_DOCKER_IMAGE, self.render_command(job), detach=True
        )
        container.wait()
        logs = container.logs(stream=False)
        decoded = logs.decode("utf-8")
        self.logger.info(decoded.strip())
        container.remove()


class RuntimeManager:
    RUNTIME_CLASSES = {
        RuntimeType.SUBPROCESS: SubprocessRuntime,
        RuntimeType.DOCKER: DockerRuntime,
    }

    def get_runtime(self, runtime: RuntimeType) -> Runtime:
        return self.RUNTIME_CLASSES[runtime]()
