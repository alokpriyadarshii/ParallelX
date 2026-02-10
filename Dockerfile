FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install -U pip && pip install -e ".[dev]"

# Default: run an example
CMD ["parallelx", "run", "examples/workflows/montecarlo_pi.json", "--max-workers", "4"]
