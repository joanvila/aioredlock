"""
This example script demonstrates how to use ``aioredlock`` with Sentinels_.
Sentinels are useful when you want to make sure that you are always hitting
the master redis instance in your cluster, even after failover.

In order to run this script, make sure to start the docker-compose setup.

.. code-block:: bash

    docker-compose up -d
    docker-compose logs -f sentinel  # to follow the logs for the sentinel to see the failover

And then in another terminal run the following command to execute this script.

.. code-block:: bash

    python -m examples.sentinel

.. note::

    If you are running on a Mac, you will need to enable TunTap_ so that the
    docker container ip addresses on the bridge are accessible from the mac
    host.

.. note::

    This example script requires that the ``example`` extras be installed.

    .. code-block:: bash

        pip install -e .[examples]

.. _Sentinels: https://redis.io/topics/sentinel
.. _TunTap: https://github.com/AlmirKadric-Published/docker-tuntap-osx
"""
import asyncio
import logging

import aiodocker

from aioredlock import Aioredlock, LockError, LockAcquiringError, Sentinel


async def get_container(name):
    docker = aiodocker.Docker()
    return await docker.containers.get(name)


async def get_container_ip(name, network=None):
    container = await get_container(name)
    return container['NetworkSettings']['Networks'][network or 'aioredlock_backend']['IPAddress']


async def lock_context():
    sentinel_ip = await get_container_ip('aioredlock_sentinel_1')

    lock_manager = Aioredlock([
        Sentinel('redis://{0}:26379/0?master=leader'.format(sentinel_ip)),
        Sentinel('redis://{0}:26379/1?master=leader'.format(sentinel_ip)),
        Sentinel('redis://{0}:26379/2?master=leader'.format(sentinel_ip)),
        Sentinel('redis://{0}:26379/3?master=leader'.format(sentinel_ip)),
    ])

    if await lock_manager.is_locked("resource"):
        print('The resource is already acquired')

    try:
        # if you dont set your lock's lock_timeout, its lifetime will be automatically extended
        async with await lock_manager.lock("resource") as lock:
            assert lock.valid is True
            assert await lock_manager.is_locked("resource") is True

            # pause leader to simulate a failing node and cause a failover
            container = await get_container('aioredlock_leader_1')
            await container.pause()

            # Do your stuff having the lock
            await asyncio.sleep(lock_manager.internal_lock_timeout * 2)
            # lock manager will extend the lock automatically
            assert await lock_manager.is_locked(lock)
            # or you can extend your lock's lifetime manually
            await lock.extend()
            # Do more stuff having the lock and if you spend much more time than you expected, the lock might be freed
            await container.unpause()

        assert lock.valid is False  # lock will be released by context manager
    except LockAcquiringError:
        print('Something happened during normal operation. We just log it.')
    except LockError:
        print('Something is really wrong and we prefer to raise the exception')
        raise

    assert lock.valid is False
    assert await lock_manager.is_locked("resource") is False

    await lock_manager.destroy()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(lock_context())
