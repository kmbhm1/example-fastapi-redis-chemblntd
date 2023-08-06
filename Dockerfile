FROM python:3.10 as python-base
RUN mkdir app
WORKDIR  /app
COPY /pyproject.toml /app
COPY /README.md /app
ADD poc_redis_fastapi_chemblntd /app/poc_redis_fastapi_chemblntd
RUN pip3 install poetry
RUN poetry config virtualenvs.create false
RUN poetry install
COPY . .
CMD ["gunicorn", "-w", "1", "-k", "uvicorn.workers.UvicornWorker", "poc_redis_fastapi_chemblntd.main:app", "--bind", "0.0.0.0:8000"]
