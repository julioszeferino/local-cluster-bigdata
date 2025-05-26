FROM jupyter/pyspark-notebook:latest

COPY requirements/requirements.txt .

RUN pip3 install -r requirements.txt