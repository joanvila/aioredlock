# Change Log

All notable changes to this project will be documented in this file.

## 0.1.0 - 2017-03-19

### Added
- Drift time check. See https://redis.io/topics/distlock#is-the-algorithm-asynchronous

### Changed
- Randomized the retry delay for trying to acquire the lock again (range between 0.1 and 0.3 seconds).

## 0.0.1 - 2017-03-05

### Added
- aioredlock first version with all the basic distributed lock manager functionalities.
