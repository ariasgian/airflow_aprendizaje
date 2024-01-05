#!/bin/bash
path=/mnt/linux/codigo/airflow_aprendizaje
pip install virtualenv
python3.10 -m venv $path/venv
source $path/venv/bin/activate
export AIRFLOW_HOME=$path/local
AIRFLOW_VERSION=2.7.3
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
python -m pip install --upgrade pip setuptools
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install pendulum
pip install apache-airflow-providers-google
pip3 install 'apache-airflow[google]'
pip install python-dotenv
pip install google-auth
