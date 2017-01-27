import asyncio
import aioredis

from aioredlock import Aioredlock


async def do_stuff_in_redis():
    pool = await aioredis.create_pool(('localhost', 6379), minsize=1, maxsize=1)

    with (await pool) as redis:    # high-level redis API instance
        await redis.set('my-key', 'value')
        await redis.expire('my-key', 1)
        val = await redis.get('my-key')

    print('retrieved value: ', val)

    pool.close()
    await pool.wait_closed()


async def basic_lock():
    lock_manager = Aioredlock('localhost', 6379)
    lock = await lock_manager.lock("resource", 10)
    assert lock.valid is True

    await do_stuff_in_redis()

    await lock.unlock()
    assert lock.valid is False


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(basic_lock())
