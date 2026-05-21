from dataclasses import dataclass

from deerflow.pipeline import CodeProcessingPipeline
from deerflow.pipeline.component_packager import ComponentPackage
from deerflow.utils.code_ast_processor import CodeASTProcessor
from deerflow.utils.code_semantic_router import CodeSemanticRouter


@dataclass
class RecordingPackager:
    calls: list[dict]

    def package(self, *, normalized_function_code, intent, io_extraction):
        self.calls.append(
            {
                "step": "package",
                "normalized_function_code": normalized_function_code,
                "intent": intent,
                "io_extraction": io_extraction,
            }
        )
        return ComponentPackage(
            function_name="generated_function",
            class_name="GeneratedFunction_Component",
            docstring="Return the fetched payload.",
            usage_example='if __name__ == "__main__":\n    print(GeneratedFunction_Component.run())',
            packaged_code='class GeneratedFunction_Component:\n    """packed"""',
        )

    def generate_docstring(self, *, normalized_function_code, intent, io_extraction):
        self.calls.append(
            {
                "step": "generate_docstring",
                "normalized_function_code": normalized_function_code,
                "intent": intent,
                "io_extraction": io_extraction,
            }
        )
        return "Return the fetched payload."

    def generate_usage_example(
        self,
        *,
        normalized_function_code,
        intent,
        io_extraction,
        class_name,
        function_name,
    ):
        self.calls.append(
            {
                "step": "generate_usage_example",
                "normalized_function_code": normalized_function_code,
                "intent": intent,
                "io_extraction": io_extraction,
                "class_name": class_name,
                "function_name": function_name,
            }
        )
        return 'if __name__ == "__main__":\n    print(GeneratedFunction_Component.run())'

    def render_component(self, *, function_code, class_name, function_name, usage_example):
        self.calls.append(
            {
                "step": "render_component",
                "function_code": function_code,
                "class_name": class_name,
                "function_name": function_name,
                "usage_example": usage_example,
            }
        )
        return 'class GeneratedFunction_Component:\n    """packed"""'

    def _function_name(self, function_code):
        return "generated_function"

    def _class_name(self, function_name):
        return "GeneratedFunction_Component"

    def _insert_docstring(self, function_code, docstring):
        return function_code.replace(":\n", f':\n    """{docstring}"""\n', 1)


def test_pipeline_normalizes_routes_extracts_io_and_packages() -> None:
    packager = RecordingPackager(calls=[])
    pipeline = CodeProcessingPipeline(
        ast_processor=CodeASTProcessor(),
        semantic_router=CodeSemanticRouter(enable_embedding=False),
        component_packager=packager,
    )

    result = pipeline.process_snippet("import requests\npayload = requests.get(url).json()")

    assert result["status"] == "success"
    assert result["metadata"] == {
        "intent": "HTTP + JSON fetch",
        "tags": ["network", "http", "api", "json"],
    }
    assert result["interface"] == {"input": ["url"], "output": ["payload"]}
    assert result["final_component_code"].startswith("class GeneratedFunction_Component")
    assert [call["step"] for call in packager.calls] == [
        "generate_docstring",
        "generate_usage_example",
        "render_component",
    ]
    assert packager.calls[0]["normalized_function_code"].startswith("def generated_function")
    assert packager.calls[0]["intent"] == "HTTP + JSON fetch"
    assert packager.calls[0]["io_extraction"] == {"input": ["url"], "output": ["payload"]}


def test_pipeline_returns_error_payload_when_a_step_fails() -> None:
    class BrokenRouter:
        def route(self, code):
            raise RuntimeError("router unavailable")

    pipeline = CodeProcessingPipeline(
        ast_processor=CodeASTProcessor(),
        semantic_router=BrokenRouter(),
        component_packager=RecordingPackager(calls=[]),
    )

    result = pipeline.process_snippet("def ok():\n    return 1")

    assert result["status"] == "error"
    assert result["step"] == "intent_and_tagging"
    assert "router unavailable" in result["error"]
    assert result["final_component_code"] == ""
