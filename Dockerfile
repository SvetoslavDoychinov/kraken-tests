FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir poetry

COPY pyproject.toml poetry.lock ./

RUN poetry install --no-root --no-interaction --no-ansi

COPY . .

ENTRYPOINT ["poetry", "run", "pytest", "-v", "tests/test_dummy.py"]
