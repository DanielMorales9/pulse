from enum import StrEnum

DEFAULT_DOCKER_IMAGE = "ubuntu"


class RuntimeType(StrEnum):
    SUBPROCESS = "subprocess"
    DOCKER = "docker"


DEFAULT_LOG_FORMAT = "[%(asctime)s] (P-%(process)s:T-%(thread)d) %(levelname)s %(filename)s:%(lineno)s %(name)s - %(message)s"
DEFAULT_MAX_PARALLELISM = 20
