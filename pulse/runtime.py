import subprocess
from abc import ABC, abstractmethod

import docker

from pulse.constants import DEFAULT_DOCKER_IMAGE, RuntimeType
from pulse.logutils import LoggingMixing
from pulse.models import Job


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


class RuntimeManager:
    RUNTIME_CLASSES = {
        RuntimeType.SUBPROCESS: SubprocessRuntime,
        RuntimeType.DOCKER: DockerRuntime,
    }

    def get_runtime(self, runtime: RuntimeType) -> Runtime:
        return self.RUNTIME_CLASSES[runtime]()
