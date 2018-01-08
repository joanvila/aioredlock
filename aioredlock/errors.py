class AioredlockError(Exception):
    """
    Base exception for aioredlock
    """


class LockError(AioredlockError):
    """
    Error in acquireing lock
    """
