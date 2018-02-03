import attr


@attr.s
class Lock:

    lock_manager = attr.ib()
    resource = attr.ib()
    id = attr.ib()
    valid = attr.ib(default=False)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.lock_manager.unlock(self)

    async def extend(self):
        await self.lock_manager.extend(self)
