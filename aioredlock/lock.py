
class Lock:

    def __init__(self, lock_manager, resource, lock_identifier, valid=False):
        """
        Initialize a lock with its fields.
        """
        self.lock_manager = lock_manager
        self.resource = resource
        self.id = lock_identifier
        self.valid = valid

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.lock_manager.unlock(self)

    async def extend(self):
        await self.lock_manager.extend(self)
