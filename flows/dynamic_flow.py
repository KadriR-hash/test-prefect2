from prefect.flows import Flow


class AbstractClassBasedFlow:
    factor: int = None
    extra: int = None

    @classmethod
    def run_config(cls, x, y):
        a = cls.example_method(x)
        b = cls.example_method(y)
        return a + b + cls.extra

    @classmethod
    def example_method(cls, value):
        return value ** cls.factor

    @classmethod
    def to_prefect_flow(cls):
        return Flow(
            fn=cls.run_config,
            name=cls.__name__,
        )


if __name__ == '__main__':
    flow1 = AbstractClassBasedFlow.to_prefect_flow()
    result1 = flow1(x=3, y=4)
