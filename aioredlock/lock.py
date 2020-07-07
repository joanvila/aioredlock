import attr


@attr.s
class Lock:

    lock_manager = attr.ib()
    resource = attr.ib()
    id = attr.ib()
    lock_timeout = attr.ib(default=10.0)
    valid = attr.ib(default=False)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.lock_manager.unlock(self)

    async def extend(self):
        await self.lock_manager.extend(self)

    async def release(self):
        await self.lock_manager.unlock(self)

    async def is_locked(self):
        return await self.lock_manager.is_locked(self)
