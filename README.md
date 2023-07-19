# Proof-of-Concept Redis/FastApi Service integration with ChEMBL data

This project has the following goals:

- Use [FastAPI](https://fastapi.tiangolo.com/tutorial/) to build a REST [API](https://github.com/redis-developer/fastapi-redis-tutorial/tree/master) in front of a [Redis](https://developer.redis.com/develop/python/fastapi) database to read data.
  - Use the Python [Redis](https://redis.io/docs/clients/python/#example-indexing-and-querying-json-documents) package to interact with the database.
  - Use the [python-redis-om](https://github.com/redis/redis-om-python/blob/main/docs/getting_started.md) package to interact with and create object models.
- Ingest ChEMBL [data](https://chembl.gitbook.io/chembl-ntd/downloads/deposited-set-7-the-harvard-medical-school-liver-stage-malaria-dataset-9th-october-2012) data & enable for `smiles` string [full-text search](https://redis.io/docs/clients/om-clients/stack-python/#find-people-using-full-text-search-on-their-personal-statements).
- Integrate [Poetry](https://python-poetry.org/docs/) & [pyenv](https://github.com/pyenv/pyenv#set-up-your-shell-environment-for-pyenv). Here is an [example](https://dev.to/nimishverma/a-guide-to-start-a-fastapi-poetry-serverless-project-142d) project using FastAPI & Poetry.
- Integrate with [docker-compose](https://scheele.hashnode.dev/build-a-dockerized-fastapi-application-with-poetry-and-gunicorn).

## Table of Contents

- [Proof-of-Concept Redis/FastApi Service integration with ChEMBL data](#proof-of-concept-redisfastapi-service-integration-with-chembl-data)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
    - [Pyenv](#pyenv)
    - [Poetry](#poetry)
    - [Redis](#redis)
    - [Fast Api](#fast-api)
    - [Docker \& Docker Compose](#docker--docker-compose)
  - [Development](#development)
    - [Run Locally](#run-locally)
    - [Run the Docker Container Locally](#run-the-docker-container-locally)
    - [Retrieve the Swagger Documentation](#retrieve-the-swagger-documentation)
  - [Example Use](#example-use)
  - [Future Work](#future-work)

## Installation

The following are dependencies for running the project. Install each. It is advised to install them in the following order.

### Pyenv

pyenv lets you easily switch between multiple versions of Python. It's simple, unobtrusive, and follows the UNIX tradition of single-purpose tools that do one thing well. [Install pyenv](https://github.com/pyenv/pyenv#installation) first. Then create the python environment using the `.python-version` file.

```shell
cd <project>                            # Change to the project directory
pyenv install $(cat .python-version)    # Install the version
```

### Poetry

[Poetry](https://python-poetry.org/docs/#installation) is a tool for dependency management and packaging in Python. It allows you to declare the libraries your project depends on and manages (install/update) them for you.

```shell
cd <project>                            # Change to the project directory
poetry init                             # Initialize the existing project
poetry shell                            # Activate the virtual environment
exit                                    # Deactivate the virtual environment, if desired
```

### Redis

Redis is an open source (BSD licensed), in-memory data structure store, used as a database, cache, and message broker. [Install Redis](https://redis.io/topics/quickstart) first, then start the Redis server using a docker container, locally. Note, docker must be [installed](https://docs.docker.com/engine/install/) first. See [below](#docker--docker-compose) for more information on docker and docker-compose.

```shell
docker run -it --rm --name redis-stack-latest \         # Run the Redis container locally
   -p 6379:6379 \                                       # Expose the Redis port
   redis/redis-stack:latest                             # Use the latest version
```

### Fast Api

Fast Api is a modern, fast (high-performance), web framework for building APIs with Python 3.6+ based on standard Python type hints. [Install fastapi](https://fastapi.tiangolo.com/#installation) (with uvicorn & gunicorn) first, then start the application. Note, the redis server must be running to use the app.

```shell
cd <project>                                            # Change to the project directory
uvicorn poc_redis_fastapi_chemblntd.main:app --reload   # Run the FastAPI server
```

### Docker & Docker Compose

[Docker](https://docs.docker.com/get-docker/) is a set of platform as a service (PaaS) products that use OS-level virtualization to deliver software in packages called containers. Containers are isolated from one another and bundle their own software, libraries and configuration files; they can communicate with each other through well-defined channels. [Docker Compose](https://docs.docker.com/compose/install/) is a tool for defining and running multi-container Docker applications. With Compose, you use a YAML file to configure your application's services. Then, with a single command, you create and start all the services from your configuration.

```shell
cd <project>                            # Change to the project directory
docker-compose up                       # Run the docker container locally
```

## Development

### Run Locally

1. Perform the installation [steps](#installation).
2. [Start](#redis) the Redis server.
3. [Start](#fast-api) the FastAPI server with reload.
4. Perform any changes to the code.
5. When finished, shut down the FastAPI server with `Ctrl+C`. Then shut down the Redis server with `Ctrl+C`.

### Run the Docker Container Locally

1. Perform the installation [steps](#installation).
2. [Run](#docker--docker-compose) the docker container locally.

### Retrieve the Swagger Documentation

Navigate to [http://localhost:8000/docs](http://localhost:8000/docs) to view the Swagger documentation while the FastAPI server is running.

## Example Use

Ideally, one would refresh the data in redis or load it. As of 7/19/2023, the refresh process pulls down the ChEMBL-ntd [data](https://chembl.gitbook.io/chembl-ntd/downloads/deposited-set-7-the-harvard-medical-school-liver-stage-malaria-dataset-9th-october-2012) to a temp file and loads it into the running redis server. It flushes the database before loading each time.  To refresh the data after starting the FastAPI server, run the following:

```shell
curl -X POST "http://localhost:8000/refresh"
```

Then, one can search for a `smiles` string in the ChEMBL-ntd [data](https://chembl.gitbook.io/chembl-ntd/downloads/deposited-set-7-the-harvard-medical-school-liver-stage-malaria-dataset-9th-october-2012) with the following:

```shell
curl -X GET "http://localhost:8000/chemblntd/search/smiles/NC(=O)" -H  "accept: application/json"
```

## Future Work

- [ ] Integrate more [caching](https://developer.redis.com/develop/python/fastapi/#caching-data-with-redis) with Redis & add the ability to add data.
- [ ] Modify the refrest process to be more dynamic.
- [ ] Create tests and test coverage.
- [ ] Update the dosstrings and OpenApi documentation.
- [ ] Add pre-commit [hooks](https://pre-commit.com/).
- [ ] Integrate different data sources & models.
- [ ] Modify the indexing model and REST methods for more robust searching.
