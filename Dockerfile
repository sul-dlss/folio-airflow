FROM apache/airflow:2.6.2-python3.10

USER root
RUN usermod -u 214 airflow
RUN apt-get update && apt-get install -y gcc git libmagic-dev

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/"
ENV SLUGIFY_USES_TEXT_UNIDECODE "yes"

USER airflow

COPY airflow.cfg requirements.txt pyproject.toml qa.sql poetry.lock ./

COPY libsys_airflow ./libsys_airflow
COPY bin ./bin

RUN pip install -r requirements.txt
RUN poetry build --format=wheel --no-interaction --no-ansi
RUN pip install dist/*.whl
