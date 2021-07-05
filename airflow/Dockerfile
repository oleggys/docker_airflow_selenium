FROM apache/airflow:2.1.0-python3.7
USER airflow
RUN pip install 'apache-airflow[celery]'
RUN pip install 'apache-airflow-providers-apache-spark'
RUN pip install 'apache-airflow-providers-mongo'
RUN pip install 'pymongo'
RUN pip install 'fake_useragent'
RUN pip install 'selenium==3.141.0'
RUN pip install 'beautifulsoup4==4.9.3'
RUN pip install 'docker==5.0.0'
RUN pip install 'lxml==4.6.3'

ARG AIRFLOW_USER_HOME=/opt/airflow
ENV PYTHONPATH=$PYTHONPATH:${AIRFLOW_USER_HOME}

USER root

RUN apt-get update -yqq && \
    apt-get install -y gnupg && \
    apt-get install -y apt-transport-https ca-certificates

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update -yqq && \
    apt-get install -y unixodbc unixodbc-dev && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    ACCEPT_EULA=Y apt-get install mssql-tools

USER root

#RUN usermod -aG 0 airflow
USER root

#RUN mkdir downloads