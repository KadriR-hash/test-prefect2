import requests
from prefect import flow, task


@task
def call_api(url):
    response = requests.get(url)
    print(response.status_code)
    return response.json()


@flow
def api_flow(url):
    fact_json = call_api(url)
    return fact_json


if __name__ == "__main__":
    print(api_flow("https://catfact.ninja/fact"))
