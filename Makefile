.PHONY: install lint typecheck test run-example

install:
	python -m pip install -U pip
	pip install -e ".[dev]"

lint:
	python -m ruff check .

typecheck:
	python -m mypy parallelx

test:
	python -m unittest discover -s tests -p 'test*.py' -v

run-example:
	parallelx run examples/workflows/montecarlo_pi.json --max-workers 4 --cache-dir .cache
