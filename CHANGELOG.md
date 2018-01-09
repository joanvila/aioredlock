# Change Log

All notable changes to this project will be documented in this file.

## 0.2.0 - 2018-01-07
### Changed
- Support for aioredis version to 1.0.0.

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
