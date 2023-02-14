import datetime
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(name="My Example Task",
      description="An example task for a tutorial.",
      tags=["tutorial", "tag-test"],
      task_run_name="hello-{name}-on-{date:%A}",
      retries=2,
      retry_delay_seconds=10,
      cache_key_fn=task_input_hash,
      cache_expiration=timedelta(minutes=1))
def my_task(name, date):
    print(f"running {name} at {date}")


@flow(flow_run_name="{name}-on-{date:%A}")
def my_flow(name: str, date: datetime.datetime):
    my_task(name=name, date=date)


# creates a flow run called 'marvin-on-Thursday'
my_flow(name="marvin", date=datetime.datetime.utcnow())
