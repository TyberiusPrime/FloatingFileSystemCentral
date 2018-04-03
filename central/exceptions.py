
class StartupNotDone(Exception):
    """To client, when the engine is still booting up"""
    pass


class EngineFaulted(Exception):
    """to client, when the engine is in ManualInterventionNeeded state"""
    pass
    
class SSHConnectFailed(Exception):
    pass


class ManualInterventionNeeded(ValueError):
    pass


class CodingError(ManualInterventionNeeded):
    pass


class InconsistencyError(ManualInterventionNeeded):
    pass


class InProgress(ValueError):
    pass


class MoveInProgress(InProgress):
    pass


class NewInProgress(InProgress):
    pass


class RemoveInProgress(InProgress):
    pass


class RenameInProgress(InProgress):
    pass


class InvalidTarget(KeyError):
    pass


class RestartError(Exception):
    pass