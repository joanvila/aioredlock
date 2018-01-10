import asyncio
from aioredlock import Aioredlock, LockError


async def lock_context():
    lock_manager = Aioredlock([
        'redis://localhost:6379/0',
        'redis://localhost:6379/1',
        'redis://localhost:6379/2',
        'redis://localhost:6379/3',
    ])

    try:
        async with await lock_manager.lock("resource") as lock:
            assert lock.valid is True
            # Do your stuff having the lock
            await lock.extend()
            # Do more stuff having the lock
        assert lock.valid is False  # lock will be released by context manager
    except LockError:
        print('"resource" key might be not empty. Please call '
              '"del resource" in redis-cli')
        raise

    await lock_manager.destroy()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(lock_context())
