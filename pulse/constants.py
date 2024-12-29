from enum import StrEnum

DEFAULT_DOCKER_IMAGE = "ubuntu"


class RuntimeType(StrEnum):
    SUBPROCESS = "subprocess"
    DOCKER = "docker"


DEFAULT_LOG_FORMAT = "[%(asctime)s] (P-%(process)s:T-%(thread)d) %(levelname)s %(filename)s:%(lineno)s %(name)s - %(message)s"
DEFAULT_MAX_PARALLELISM = 20


class JobExecutorType(StrEnum):
    THREAD = "thread"
    PROCESS = "process"


class JobRunStatus(StrEnum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


UNFINISHED_JOB_RUN_STATES = (JobRunStatus.RUNNING, JobRunStatus.FAILED)
DEFAULT_SCHEDULE = "0 * * * *"
