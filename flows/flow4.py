from prefect import flow, task
from pydantic import BaseModel


class Model(BaseModel):
    a: int
    b: float
    c: str


@task
def printer(obj):
    print(f"Received a {type(obj)} with value {obj}")


@flow(name="My Example Flow",
      description="An example flow for a tutorial.",
      version="v02")
# version=os.getenv("GIT_COMMIT_SHA")
# if u are using git to version control
def model_validator(model: Model):
    printer(model)


if __name__ == '__main__':
    model_validator({"a": 42, "b": 0, "c": 55})
