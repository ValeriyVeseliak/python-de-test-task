FROM python:3.8-slim-buster

WORKDIR /usr/src/migrator
RUN pip install flask pyyaml psycopg2-binary mysql-connector-python
RUN mkdir /home/logs
ADD main.py .
ADD migrator_config.yaml .
ADD schema_mapping_config.yaml .

CMD ["python", "main.py"]
