import csv
import logging
import pandas as pd
from datetime import datetime
from prefect import task, flow
import psutil


@task
def extract():
    """res = requests.get(url)
    if not res:
        raise Exception('No data fetched!')
    return json.loads(res.content)"""
    process = psutil.Process()
    start_cpu_usage = process.cpu_percent()
    start_mem_usage = process.memory_info().rss

    with open('./data/traffic.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        rows = []
        for row in reader:
            rows.append(row)
        rows.pop(0)

    end_cpu_usage = process.cpu_percent()
    end_mem_usage = process.memory_info().rss
    cpu_usage_percent = end_cpu_usage - start_cpu_usage

    mem_usage_mb = (end_mem_usage - start_mem_usage) / 1024 / 1024
    with open('./benchmark/log.txt', 'a+') as f:
        f.write(f"CPU Usage: {cpu_usage_percent}% ; Memory Usage: {mem_usage_mb} MB \n")
        logging.warning(f"CPU Usage: {cpu_usage_percent}% ; Memory Usage: {mem_usage_mb} MB")

    return rows


@task
def transform(data):
    """transformed = []
    for user in data:
        transformed.append({
            'ID': user['id'],
            'Name': user['name'],
            'Username': user['username'],
            'Email': user['email'],
            'Address': f"{user['address']['street']}, {user['address']['suite']}, {user['address']['city']}",
            'PhoneNumber': user['phone'],
            'Company': user['company']['name']
        })
    return pd.DataFrame(transformed)"""
    process = psutil.Process()
    start_cpu_usage = process.cpu_percent()
    start_mem_usage = process.memory_info().rss

    transformed = []
    for line in data:
        transformed.append({
            'Date': line[0],
            'Category': line[1],
            'Browser': line[2],
            'Sessions': line[4],
            'Visitors': line[3],
        })
    end_cpu_usage = process.cpu_percent()
    end_mem_usage = process.memory_info().rss
    cpu_usage_percent = end_cpu_usage - start_cpu_usage

    mem_usage_mb = (end_mem_usage - start_mem_usage) / 1024 / 1024
    with open('./benchmark/log.txt', 'a+') as f:
        f.write(f"CPU Usage: {cpu_usage_percent}% ; Memory Usage: {mem_usage_mb} MB \n")
        logging.warning(f"CPU Usage: {cpu_usage_percent}% ; Memory Usage: {mem_usage_mb} MB")

    return pd.DataFrame(transformed)


@task
def load(data):
    """data.to_csv(path_or_buf=path, index=False)"""
    process = psutil.Process()
    start_cpu_usage = process.cpu_percent()
    start_mem_usage = process.memory_info().rss

    csv_path = "./data/transformed.csv"
    data.to_csv(path_or_buf=csv_path, index=False)

    end_cpu_usage = process.cpu_percent()
    end_mem_usage = process.memory_info().rss
    cpu_usage_percent = end_cpu_usage - start_cpu_usage

    mem_usage_mb = (end_mem_usage - start_mem_usage) / 1024 / 1024
    with open('./benchmark/log.txt', 'a+') as f:
        f.write(f"CPU Usage: {cpu_usage_percent}% ; Memory Usage: {mem_usage_mb} MB \n")
        logging.warning(f"CPU Usage: {cpu_usage_percent}% ; Memory Usage: {mem_usage_mb} MB")
    return csv_path


@flow
def prefect_flow():
    rows = extract()
    df_users = transform(rows)
    load(df_users)


if __name__ == '__main__':
    prefect_flow()
