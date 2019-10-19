class AioredlockError(Exception):
    """
    Base exception for aioredlock
    """


class LockError(AioredlockError):
    """
    Error in acquiring or releasing the lock
    """
