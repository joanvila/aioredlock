.PHONY: flake8 ut acceptance all_tests coverage

syntax:
	flake8

ut:
	coverage run -m pytest -sv tests/ut

acceptance:
	pytest -sv tests/acceptance

all_tests: syntax ut acceptance coverage

coverage:
	coverage report
	coverage xml
