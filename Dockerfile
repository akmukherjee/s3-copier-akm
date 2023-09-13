FROM prefecthq/prefect:2.13.0-python3.11
COPY requirements.txt /opt/prefect/s3-copier/requirements.txt
RUN python -m pip install -r /opt/prefect/s3-copier/requirements.txt
COPY . /opt/prefect/s3-copier/
WORKDIR /opt/prefect/s3-copier/
