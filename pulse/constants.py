from enum import StrEnum

DEFAULT_DOCKER_IMAGE = "ubuntu"


class RuntimeType(StrEnum):
    SUBPROCESS = "subprocess"
    DOCKER = "docker"
