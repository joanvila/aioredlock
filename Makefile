.PHONY: flake8 ut acceptance all_tests

syntax:
	flake8

ut:
	pytest -sv tests/ut

acceptance:
	docker-compose -f docker-compose.yml up -d
	pytest -sv tests/acceptance
	docker-compose -f docker-compose.yml stop

all_tests: syntax ut acceptance
