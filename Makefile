.PHONY: flake8 ut acceptance all_tests coverage

syntax:
	flake8

ut:
	coverage run -p -m pytest -v tests/ut

acceptance:
	coverage run -p -m pytest -v tests/acceptance

all_tests: syntax ut acceptance coverage

coverage:
	coverage combine
	coverage report
	coverage xml

clean:
	find . -name \*.pyc -delete
	find . -name __pycache__ -exec rmdir {} +
	rm -rf htmlcov/ aioredlock.egg-info/ dist/ build/ .pytest_cache/ coverage.xml
	git checkout -- files/redis/sentinel.conf
	coverage erase
