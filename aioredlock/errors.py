class AioredlockError(Exception):
    """
    Base exception for aioredlock
    """


class LockError(AioredlockError):
    """
    Error in acquiring or releasing the lock
    """


class LockAcquiringError(LockError):
    """
    Error in acquiring the lock during normal operation
    """


class LockRuntimeError(LockError):
    """
    Error in acquiring or releasing the lock due to an unexpected event
    """
