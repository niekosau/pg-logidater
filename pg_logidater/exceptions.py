class PsqlConnectionError(Exception):
    def __init__(self, message=None):
        if not message:
            message = "Unable to connect"
        super().__init__(message)


class DatabaseExists(Exception):
    def __init__(self, message=None):
        if not message:
            message = "Database exists"
        super().__init__(message)


class ReplicaPaused(Exception):
    def __init__(self, message=None):
        if not message:
            message = "Replica paused"
        super().__init__(message)


class ReplicaLevelNotCorrect(Exception):
    def __init__(self, message=None):
        if not message:
            message = "wal_level not correct"
        super().__init__(message)


class ReplicaSlotExists(Exception):
    def __init__(self, message=None):
        if not message:
            message = "Replication slot exist"
        super().__init__(message)


class PublicationExists(Exception):
    def __init__(self, message=None):
        if not message:
            message = "Publication exists"
        super().__init__(message)


class VersionNotSupported(Exception):
    def __init__(self, message=None):
        if not message:
            message = "Version not supported"
        super().__init__(message)


class DiskSpaceTooLow(Exception):
    pass
