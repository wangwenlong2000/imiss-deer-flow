"""Top-level orchestration for automated code snippet processing."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Protocol, TypedDict

from deerflow.pipeline.component_packager import ComponentPackage
from deerflow.utils.code_ast_processor import IOExtractionResult, ValidationResult
from deerflow.utils.code_semantic_router import CodeSemanticResult

logger = logging.getLogger(__name__)


class _PipelineStepError(RuntimeError):
    def __init__(self, step: str, error: Exception) -> None:
        super().__init__(str(error))
        self.step = step


class _ASTProcessor(Protocol):
    def validate_format(self, code: str) -> ValidationResult: ...

    def normalize_code(self, code: str, function_name: str = "generated_function") -> str: ...

    def extract_io(self, code: str) -> IOExtractionResult: ...


class _SemanticRouter(Protocol):
    def route(self, code: str) -> CodeSemanticResult: ...


class _ComponentPackager(Protocol):
    def package(
        self,
        *,
        normalized_function_code: str,
        intent: str,
        io_extraction: Mapping[str, Any],
    ) -> ComponentPackage: ...


class _GranularComponentPackager(_ComponentPackager, Protocol):
    def generate_docstring(
        self,
        *,
        normalized_function_code: str,
        intent: str,
        io_extraction: Mapping[str, Any],
    ) -> str: ...

    def generate_usage_example(
        self,
        *,
        normalized_function_code: str,
        intent: str,
        io_extraction: Mapping[str, Any],
        class_name: str,
        function_name: str,
    ) -> str: ...

    def render_component(
        self,
        *,
        function_code: str,
        class_name: str,
        function_name: str,
        usage_example: str,
    ) -> str: ...

    def _function_name(self, function_code: str) -> str: ...

    def _class_name(self, function_name: str) -> str: ...

    def _insert_docstring(self, function_code: str, docstring: str) -> str: ...


class CodeProcessingSuccess(TypedDict):
    status: str
    metadata: CodeSemanticResult
    interface: IOExtractionResult
    final_component_code: str


class CodeProcessingError(TypedDict, total=False):
    status: str
    step: str
    error: str
    metadata: CodeSemanticResult
    interface: IOExtractionResult
    final_component_code: str


class CodeProcessingPipeline:
    """Run static analysis, semantic routing, and component packaging in order."""

    def __init__(
        self,
        *,
        ast_processor: _ASTProcessor,
        semantic_router: _SemanticRouter,
        component_packager: _ComponentPackager,
        logger_: logging.Logger | None = None,
    ) -> None:
        self.ast_processor = ast_processor
        self.semantic_router = semantic_router
        self.component_packager = component_packager
        self.logger = logger_ or logger

    def process_snippet(self, raw_code: str) -> CodeProcessingSuccess | CodeProcessingError:
        """Process a Python snippet and return a unified JSON-serializable payload."""

        normalized_code = raw_code
        metadata: CodeSemanticResult = {"intent": "unknown", "tags": []}
        interface: IOExtractionResult = {"input": [], "output": []}

        try:
            self._ensure_non_empty(raw_code)

            step = "format_validation"
            self.logger.info("Code pipeline step started: %s", step)
            validation = self.ast_processor.validate_format(raw_code)
            self.logger.info("Code pipeline step completed: %s valid=%s", step, validation["valid"])

            if not validation["valid"]:
                step = "normalization"
                self.logger.info(
                    "Code pipeline validation failed, attempting normalization: %s",
                    validation["error"],
                )
                normalized_code = self.ast_processor.normalize_code(raw_code)

                self.logger.info("Code pipeline step started: format_validation_after_normalization")
                normalized_validation = self.ast_processor.validate_format(normalized_code)
                if not normalized_validation["valid"]:
                    raise ValueError(
                        "normalized code failed format validation: "
                        f"{normalized_validation['error']}"
                    )
                self.logger.info("Code pipeline normalization completed successfully")

            step = "intent_and_tagging"
            self.logger.info("Code pipeline step started: %s", step)
            metadata = self.semantic_router.route(normalized_code)
            self.logger.info(
                "Code pipeline step completed: %s intent=%s tags=%s",
                step,
                metadata["intent"],
                metadata["tags"],
            )

            step = "io_extraction"
            self.logger.info("Code pipeline step started: %s", step)
            interface = self.ast_processor.extract_io(normalized_code)
            self.logger.info("Code pipeline step completed: %s interface=%s", step, interface)

            step = "doc_generation"
            self.logger.info("Code pipeline step started: %s", step)
            if self._supports_granular_packaging(self.component_packager):
                package = self._package_granular(
                    packager=self.component_packager,
                    normalized_code=normalized_code,
                    metadata=metadata,
                    interface=interface,
                )
            else:
                self.logger.info(
                    "Component packager does not expose granular generation hooks; "
                    "falling back to package()."
                )
                package = self._package_monolithic(
                    normalized_code=normalized_code,
                    metadata=metadata,
                    interface=interface,
                )

            return {
                "status": "success",
                "metadata": metadata,
                "interface": interface,
                "final_component_code": package.packaged_code,
            }
        except Exception as exc:
            failed_step = getattr(exc, "step", locals().get("step", "initialization"))
            self.logger.exception("Code pipeline failed at step %s", failed_step)
            return {
                "status": "error",
                "step": str(failed_step),
                "error": str(exc),
                "metadata": metadata,
                "interface": interface,
                "final_component_code": "",
            }

    @staticmethod
    def _ensure_non_empty(raw_code: str) -> None:
        if not isinstance(raw_code, str) or not raw_code.strip():
            raise ValueError("raw_code must be a non-empty string")

    @staticmethod
    def _supports_granular_packaging(packager: _ComponentPackager) -> bool:
        return all(
            callable(getattr(packager, name, None))
            for name in (
                "generate_docstring",
                "generate_usage_example",
                "render_component",
                "_function_name",
                "_class_name",
                "_insert_docstring",
            )
        )

    def _package_granular(
        self,
        *,
        packager: _GranularComponentPackager,
        normalized_code: str,
        metadata: CodeSemanticResult,
        interface: IOExtractionResult,
    ) -> ComponentPackage:
        function_name = packager._function_name(normalized_code)
        class_name = packager._class_name(function_name)

        try:
            docstring = packager.generate_docstring(
                normalized_function_code=normalized_code,
                intent=metadata["intent"],
                io_extraction=interface,
            )
        except Exception as exc:
            raise _PipelineStepError("doc_generation", exc) from exc
        self.logger.info("Code pipeline step completed: doc_generation")

        function_with_docstring = packager._insert_docstring(normalized_code, docstring)

        self.logger.info("Code pipeline step started: example_generation")
        try:
            usage_example = packager.generate_usage_example(
                normalized_function_code=function_with_docstring,
                intent=metadata["intent"],
                io_extraction=interface,
                class_name=class_name,
                function_name=function_name,
            )
        except Exception as exc:
            raise _PipelineStepError("example_generation", exc) from exc
        self.logger.info("Code pipeline step completed: example_generation")

        self.logger.info("Code pipeline step started: component_packaging")
        try:
            packaged_code = packager.render_component(
                function_code=function_with_docstring,
                class_name=class_name,
                function_name=function_name,
                usage_example=usage_example,
            )
        except Exception as exc:
            raise _PipelineStepError("component_packaging", exc) from exc
        self.logger.info("Code pipeline step completed: component_packaging component=%s", class_name)

        return ComponentPackage(
            function_name=function_name,
            class_name=class_name,
            docstring=docstring,
            usage_example=usage_example,
            packaged_code=packaged_code,
        )

    def _package_monolithic(
        self,
        *,
        normalized_code: str,
        metadata: CodeSemanticResult,
        interface: IOExtractionResult,
    ) -> ComponentPackage:
        self.logger.info("Code pipeline step completed by package(): doc_generation")
        self.logger.info("Code pipeline step started: example_generation")
        self.logger.info("Code pipeline step completed by package(): example_generation")
        self.logger.info("Code pipeline step started: component_packaging")
        try:
            package = self.component_packager.package(
                normalized_function_code=normalized_code,
                intent=metadata["intent"],
                io_extraction=interface,
            )
        except Exception as exc:
            raise _PipelineStepError("component_packaging", exc) from exc
        self.logger.info(
            "Code pipeline step completed: component_packaging component=%s",
            package.class_name,
        )
        return package
