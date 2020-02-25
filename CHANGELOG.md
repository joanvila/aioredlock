# Change Log

All notable changes to this project will be documented in this file.

##0.4.0 - 2020-01-08

### Added
- Add watchdogs to extend lock's lifetime automatically

### Changed
- Aioredlock totally becomes a lock manager, it track locks' state.When Aioredlock is destroyed, it releases all locks it have
- Remove ``lock_timeout`` and ``drift`` from Aioredlock's initialization parameter list and move them to ``Aioredlock.lock``
- Add ``internal_lock_timeout`` as default lock_timeout for Aioredlock initialization
- ``Aioredlock.lock(resource)`` is replaced by ``Aioredlock.lock(resource_name, lock_timeout)`` 
- ``Aioredlock.extend(lock)`` is replaced by ``Aioredlock.extend(lock, lock_timeout)``
- Remove lock_timeout from Redis' initialization parameter list
 
## 0.3.0 - 2019-01-15

### Added
- Support support for initializing aioredlock with an existing aioredis pool

## 0.2.1 - 2018-09-07

### Changed
- Move all closing logic from Redis.clear_connections() to Instance.close()
- Fixes for new version of asynctest

## 0.2.0 - 2018-03-24
### Changed
- Support for aioredis version to 1.0.0.
- Complite lock refactoring using lua scripts .
- ``Aioredlock.extend(lock)`` is implemented to extend the lock lifetime.
- The lock manager now raises the ``LockError`` exceptions if it can not lock, extend or unlock the resource.
- The lock now can be released with async context manager.
- Support the same address formats as aioredis does.
- ``Aioredlock.is_locked()`` is implemented to check is the resource acquired by another redlock instance.
- The ``lock_timeout`` and ``drift`` parameters now mesured in seconds, just like ``retry_delay_min`` and ``retry_delay_max``.

## 0.1.1 - 2017-05-14

### Changed
- Updated aioredis requirement version to 0.3.1.
- Incremented the maxsize connections pool from default to 100.

### Fixed
- Fixed a bug regarding the asyncio lock usage when creating the connection pool with redis.

## 0.1.0 - 2017-03-19

### Added
- Drift time check. See https://redis.io/topics/distlock#is-the-algorithm-asynchronous

### Changed
- Randomized the retry delay for trying to acquire the lock again (range between 0.1 and 0.3 seconds).

## 0.0.1 - 2017-03-05

### Added
- aioredlock first version with all the basic distributed lock manager functionalities.
