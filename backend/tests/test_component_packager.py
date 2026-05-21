import json
from types import SimpleNamespace

from deerflow.pipeline import ComponentPackager


class FakeChatCompletions:
    def __init__(self) -> None:
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) == 1:
            content = {
                "docstring": (
                    "Calculate the total price.\n\n"
                    "Args:\n"
                    "    prices: Item prices.\n"
                    "    tax_rate: Tax rate as a decimal.\n\n"
                    "Returns:\n"
                    "    The total price including tax."
                )
            }
        else:
            content = {
                "usage_example": (
                    'if __name__ == "__main__":\n'
                    "    total = CalculateTotal_Component.run([10.0, 20.0], tax_rate=0.1)\n"
                    "    print(total)"
                )
            }
        return SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(content=json.dumps(content)),
                )
            ]
        )


class FakeClient:
    def __init__(self) -> None:
        self.completions = FakeChatCompletions()
        self.chat = SimpleNamespace(completions=self.completions)


def test_component_packager_generates_importable_component() -> None:
    client = FakeClient()
    packager = ComponentPackager(client=client, model="mock-model")

    package = packager.package(
        normalized_function_code=(
            "def calculate_total(prices: list[float], tax_rate: float = 0.0) -> float:\n"
            "    subtotal = sum(prices)\n"
            "    return subtotal * (1 + tax_rate)\n"
        ),
        intent="Calculate the total price from item prices and a tax rate.",
        io_extraction={
            "inputs": [
                {"name": "prices", "type": "list[float]"},
                {"name": "tax_rate", "type": "float", "default": 0.0},
            ],
            "outputs": [{"type": "float"}],
        },
    )

    assert package.function_name == "calculate_total"
    assert package.class_name == "CalculateTotal_Component"
    assert "Args:" in package.docstring
    assert 'response_format' in client.completions.calls[0]
    assert client.completions.calls[0]["response_format"] == {"type": "json_object"}
    assert client.completions.calls[1]["response_format"] == {"type": "json_object"}

    namespace = {}
    exec(package.packaged_code, namespace)
    assert namespace["CalculateTotal_Component"].run([10.0, 20.0], tax_rate=0.1) == 33.0
