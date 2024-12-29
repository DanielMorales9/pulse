import subprocess
from abc import ABC, abstractmethod

import docker

from pulse.constants import DEFAULT_DOCKER_IMAGE, RuntimeType
from pulse.logutils import LoggingMixing
from pulse.models import Task


class TaskExecutionError(RuntimeError):
    def __init__(self, job_run_id: str):
        super().__init__(f"Task for job run {job_run_id}")
        self.job_run_id = job_run_id


class Runtime(ABC):
    @abstractmethod
    def run(self, task: Task) -> None:
        pass


class SubprocessRuntime(Runtime, LoggingMixing):
    def run(self, task: Task) -> None:
        self.logger.debug("Running command %s", task.command)
        try:
            process = subprocess.run(
                task.command,
                check=True,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for line in process.stdout.splitlines():
                log = line.strip()
                self.logger.info(log)
        except subprocess.CalledProcessError as e:
            raise TaskExecutionError(task.job_run_id) from e


class DockerRuntime(Runtime, LoggingMixing):
    def __init__(self) -> None:
        super().__init__()
        self.client = docker.from_env()

    def run(self, task: Task) -> None:
        self.logger.debug("Running command %s", task.command)
        container = self.client.containers.run(
            DEFAULT_DOCKER_IMAGE, task.command, detach=True
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
