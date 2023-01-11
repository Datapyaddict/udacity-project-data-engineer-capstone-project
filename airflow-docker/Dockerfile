# Base Image
FROM apache/airflow:2.3.4
LABEL maintainer="atn"

# Arguments that can be set with docker build
ARG AIRFLOW_HOME=/opt/airflow

# Export the environment variable AIRFLOW_HOME where airflow will be installed
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

USER root

# Install dependencies and tools
RUN apt-get update
RUN apt-get install -y --no-install-recommends vim
RUN apt-get autoremove -yqq --purge 
RUN apt-get clean 
RUN rm -rf /var/lib/apt/lists/*

USER airflow        

RUN pip install --upgrade --force-reinstall pip
RUN pip install apache-airflow-providers-amazon


# Create the folder dags inside $AIRFLOW_HOME
RUN mkdir -p ${AIRFLOW_HOME}/dags
RUN mkdir -p ${AIRFLOW_HOME}/config
RUN mkdir -p ${AIRFLOW_HOME}/logs
COPY ./config/airflow.cfg ${AIRFLOW_HOME}/config/airflow.cfg

# Expose ports (just to indicate that this container needs to map port)
EXPOSE 8080
