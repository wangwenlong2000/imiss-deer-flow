"""Tests for semantic routing of Python code snippets."""

from deerflow.utils.code_semantic_router import CodeSemanticRouter


class FakeEmbeddingModel:
    def __init__(self, model_name: str) -> None:
        self.model_name = model_name

    def encode(self, value: str | list[str]) -> list[float] | list[list[float]]:
        if isinstance(value, list):
            return [self._encode_one(item) for item in value]
        return self._encode_one(value)

    def _encode_one(self, value: str) -> list[float]:
        lowered = value.lower()
        if any(term in lowered for term in ("serial", "sensor", "device", "command bytes")):
            return [0.0, 0.0, 0.0, 1.0]
        if any(term in lowered for term in ("image", "frame", "pixels", "contours")):
            return [0.0, 0.0, 1.0, 0.0]
        if any(term in lowered for term in ("rows", "columns", "aggregate", "metrics")):
            return [0.0, 1.0, 0.0, 0.0]
        return [1.0, 0.0, 0.0, 0.0]


def test_rule_layer_detects_http_json_fetch() -> None:
    router = CodeSemanticRouter(enable_embedding=False)

    result = router.route("import requests\npayload = requests.get(url).json()")

    assert result == {"intent": "HTTP + JSON fetch", "tags": ["network", "http", "api", "json"]}


def test_rule_layer_detects_pandas_data_processing() -> None:
    router = CodeSemanticRouter(enable_embedding=False)

    result = router.route("import pandas as pd\ndf = pd.read_csv(path).dropna()")

    assert result == {"intent": "Data Processing", "tags": ["data", "dataframe", "etl"]}


def test_rule_layer_detects_cv2_vision() -> None:
    router = CodeSemanticRouter(enable_embedding=False)

    result = router.route("import cv2\nframe = cv2.imread(path)")

    assert result == {"intent": "Vision / Image Processing", "tags": ["vision", "image", "opencv"]}


def test_embedding_fallback_uses_singleton_model_and_nearest_prototype() -> None:
    CodeSemanticRouter._models.pop("fake-semantic-model", None)
    CodeSemanticRouter._prototype_embeddings.pop("fake-semantic-model", None)
    calls: list[str] = []

    def factory(model_name: str) -> FakeEmbeddingModel:
        calls.append(model_name)
        return FakeEmbeddingModel(model_name)

    first = CodeSemanticRouter(model_name="fake-semantic-model", model_factory=factory)
    second = CodeSemanticRouter(model_name="fake-semantic-model", model_factory=factory)

    result = first.route("port.write(command); raw = port.read(sensor_bytes)")

    assert result == {"intent": "Hardware I/O control", "tags": ["hardware", "io", "device"]}
    assert second._model is first._model
    assert calls == ["fake-semantic-model"]


def test_embedding_result_can_be_rendered_by_local_llm_generator() -> None:
    CodeSemanticRouter._models.pop("fake-llm-model", None)
    CodeSemanticRouter._prototype_embeddings.pop("fake-llm-model", None)

    def generator(code: str, candidate: dict[str, list[str] | str]) -> dict[str, list[str] | str]:
        assert "sensor_bytes" in code
        assert candidate["intent"] == "Hardware I/O control"
        return {"intent": "Serial sensor read", "tags": ["hardware", "serial", "sensor"]}

    router = CodeSemanticRouter(
        model_name="fake-llm-model",
        llm_generator=generator,
        model_factory=FakeEmbeddingModel,
    )

    assert router.route("port.write(command); raw = port.read(sensor_bytes)") == {
        "intent": "Serial sensor read",
        "tags": ["hardware", "serial", "sensor"],
    }
