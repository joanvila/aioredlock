.PHONY: flake8 ut acceptance all_tests coverage

syntax:
	flake8

ut:
	pytest -sv tests/ut

acceptance:
	pytest -sv tests/acceptance

all_tests: syntax ut acceptance

coverage:
	pytest --cov-report term-missing --cov=aioredlock -sv tests/ut
