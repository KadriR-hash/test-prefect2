import asyncio

from prefect import task, flow


@task
async def print_values(values):
    for value in values:
        await asyncio.sleep(1)  # yield
        print(value, end=" ")


@flow
async def async_flow():
    await print_values([1, 2])  # runs immediately
    await print_values.submit("abcd")
    await print_values.submit("6789")


asyncio.run(async_flow())
