FROM prefecthq/prefect:2.8.0-python3.10

COPY requirements.txt .

RUN pip install -r requirements.txt