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


class NoMainAvailable(Exception):
    def __str__(self):
        return 'NoMainAvailable'
    def __repr__(self):
        return 'NoMainAvailable()'



class NodeIsReadonly(Exception):
    def __init__(self, node, kind=""):
        Exception.__init__(
            self, "%s%s is readonly" % (node, "(%s)" % kind if kind else "")
        )
