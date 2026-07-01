# kraken-tests

## Introduction

Hello, Kraken team!
Hopefully it's the Kraken team reading this documentation.
This README file is where I expect you to start reviewing my assessment, and it contains some basic information about the project structure, dependencies and usage.

## Project Structure

```
kraken-tests/
|- tests/
|  |- __init__.py
|  |- conftest.py
|  |- helpers.py
|  |- test_book.py
|  |- test_generic_operations.py
|  |- test_ticker.py
|  |- test_trade.py
|- .dockerignore
|- Dockerfile
|- NOTES.txt
|- poetry.lock
|- pyproject.toml
|- pytest.ini
|- README.md
```

As you can see the project is a pretty standard and simple pytest project. I've also included all the required files for the assessment.
For dependency management I've decided to use poetry as it's the dependency manager I've been working with for the last 2 years.

## Dependencies

As per requirements I've kept external libraries to an absolute minimum using only `websockets`, `pytest-asyncio` and `pytest`.

## Usage

From the assignment I assume that you will be running the project in a docker container if so then typing the following commands should be sufficient as long as the docker daemon is running.

```bash
docker build -t kraken-ws-tests .
```

```bash
docker docker run --rm kraken-ws-tests
```

This should give you a relatively user-friendly output.


## Repository metadata

I've uploaded the entire project to GitHub and you can review all my commits there at:

- Repository URL: https://github.com/SvetoslavDoychinov/kraken-tests
- Branch: `main`
