"""LLM-assisted packaging for generated Python function components."""

from __future__ import annotations

import ast
import json
import logging
import textwrap
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from openai import OpenAI, OpenAIError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ComponentPackage:
    """Generated component artifacts."""

    function_name: str
    class_name: str
    docstring: str
    usage_example: str
    packaged_code: str


class ComponentPackager:
    """Generate docstrings, examples, and importable class wrappers for functions."""

    DOCSTRING_SYSTEM_PROMPT: Final[str] = (
        "You are a senior Python engineer. Return only a JSON object with a "
        '"docstring" string field. The docstring must use Google Style Python '
        "format, describe Args and Returns from the supplied IO extraction, "
        "and must not include enclosing triple quotes."
    )
    USAGE_SYSTEM_PROMPT: Final[str] = (
        "You are a senior Python engineer. Return only a JSON object with a "
        '"usage_example" string field. The value must be directly runnable '
        'Python code beginning with if __name__ == "__main__": and should call '
        "the generated component class."
    )

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        model: str = "gpt-4o-mini",
        timeout: float | None = 30.0,
        client: Any | None = None,
    ) -> None:
        """Initialize an OpenAI-compatible DeerFlow LLM client."""

        self.model = model
        self.client = client or OpenAI(
            api_key=api_key,
            base_url=base_url,
            timeout=timeout,
        )

    def package(
        self,
        *,
        normalized_function_code: str,
        intent: str,
        io_extraction: Mapping[str, Any],
    ) -> ComponentPackage:
        """Run the non-deterministic LLM steps, then deterministic packaging."""

        function_name = self._function_name(normalized_function_code)
        class_name = self._class_name(function_name)

        docstring = self.generate_docstring(
            normalized_function_code=normalized_function_code,
            intent=intent,
            io_extraction=io_extraction,
        )
        function_with_docstring = self._insert_docstring(normalized_function_code, docstring)

        usage_example = self.generate_usage_example(
            normalized_function_code=function_with_docstring,
            intent=intent,
            io_extraction=io_extraction,
            class_name=class_name,
            function_name=function_name,
        )
        packaged_code = self.render_component(
            function_code=function_with_docstring,
            class_name=class_name,
            function_name=function_name,
            usage_example=usage_example,
        )

        return ComponentPackage(
            function_name=function_name,
            class_name=class_name,
            docstring=docstring,
            usage_example=usage_example,
            packaged_code=packaged_code,
        )

    def generate_docstring(
        self,
        *,
        normalized_function_code: str,
        intent: str,
        io_extraction: Mapping[str, Any],
    ) -> str:
        """Non-deterministically generate a Google Style docstring via LLM."""

        content = self._call_json_llm(
            system_prompt=self.DOCSTRING_SYSTEM_PROMPT,
            user_payload={
                "task": "Generate a Google Style Python docstring.",
                "intent": intent,
                "io_extraction": dict(io_extraction),
                "normalized_function_code": normalized_function_code,
                "constraints": [
                    "Return JSON only.",
                    "The docstring field must not include triple quote delimiters.",
                    "Use Args and Returns sections when inputs/outputs exist.",
                ],
            },
        )
        docstring = content.get("docstring")
        if not isinstance(docstring, str) or not docstring.strip():
            raise ValueError("LLM docstring response must contain a non-empty 'docstring' string.")
        return self._clean_generated_block(docstring)

    def generate_usage_example(
        self,
        *,
        normalized_function_code: str,
        intent: str,
        io_extraction: Mapping[str, Any],
        class_name: str,
        function_name: str,
    ) -> str:
        """Non-deterministically generate a runnable usage example via LLM."""

        content = self._call_json_llm(
            system_prompt=self.USAGE_SYSTEM_PROMPT,
            user_payload={
                "task": "Generate a runnable Python usage example.",
                "intent": intent,
                "io_extraction": dict(io_extraction),
                "normalized_function_code": normalized_function_code,
                "component_class_name": class_name,
                "component_entrypoint": f"{class_name}.run",
                "wrapped_function_name": function_name,
                "constraints": [
                    "Return JSON only.",
                    'The usage_example field must start with: if __name__ == "__main__":',
                    "Do not import the component; the example is appended to the same module.",
                ],
            },
        )
        usage_example = content.get("usage_example")
        if not isinstance(usage_example, str) or not usage_example.strip():
            raise ValueError("LLM usage response must contain a non-empty 'usage_example' string.")
        cleaned = self._clean_generated_block(usage_example)
        if not cleaned.startswith('if __name__ == "__main__":'):
            raise ValueError('LLM usage example must start with if __name__ == "__main__":.')
        return cleaned

    def render_component(
        self,
        *,
        function_code: str,
        class_name: str,
        function_name: str,
        usage_example: str,
    ) -> str:
        """Deterministically render the importable component class."""

        function_body = textwrap.indent(textwrap.dedent(function_code).strip(), "    ")
        example = textwrap.dedent(usage_example).strip()
        return (
            f"class {class_name}:\n"
            f'    """Importable component wrapper for {function_name}."""\n\n'
            "    @staticmethod\n"
            f"{function_body}\n\n"
            "    @classmethod\n"
            "    def run(cls, *args, **kwargs):\n"
            f"        return cls.{function_name}(*args, **kwargs)\n\n\n"
            f"{example}\n"
        )

    def _call_json_llm(
        self,
        *,
        system_prompt: str,
        user_payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
                ],
                response_format={"type": "json_object"},
                temperature=0.1,
            )
            content = response.choices[0].message.content
        except (OpenAIError, TimeoutError, OSError, IndexError, AttributeError) as exc:
            logger.warning("Component packaging LLM request failed: %s", exc)
            raise

        if not content:
            raise ValueError("Component packaging LLM returned empty content.")

        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as exc:
            raise ValueError("Component packaging LLM returned invalid JSON.") from exc
        if not isinstance(parsed, dict):
            raise ValueError("Component packaging LLM JSON response must be an object.")
        return parsed

    def _function_name(self, function_code: str) -> str:
        module = ast.parse(textwrap.dedent(function_code))
        functions = [node for node in module.body if isinstance(node, ast.FunctionDef)]
        if len(functions) != 1:
            raise ValueError("normalized_function_code must contain exactly one top-level function.")
        return functions[0].name

    def _class_name(self, function_name: str) -> str:
        name = "".join(part.capitalize() for part in function_name.split("_") if part)
        return f"{name}_Component"

    def _insert_docstring(self, function_code: str, docstring: str) -> str:
        source = textwrap.dedent(function_code).strip()
        module = ast.parse(source)
        function = next(node for node in module.body if isinstance(node, ast.FunctionDef))
        lines = source.splitlines()
        indent = " " * (function.col_offset + 4)
        quoted_docstring = self._quote_docstring(docstring, indent)
        insertion_index = function.body[0].lineno - 1

        if ast.get_docstring(function, clean=False):
            existing = function.body[0]
            end_lineno = existing.end_lineno or existing.lineno
            return "\n".join(
                [
                    *lines[: existing.lineno - 1],
                    quoted_docstring,
                    *lines[end_lineno:],
                ]
            )

        return "\n".join(
            [
                *lines[:insertion_index],
                quoted_docstring,
                *lines[insertion_index:],
            ]
        )

    def _quote_docstring(self, docstring: str, indent: str) -> str:
        safe_docstring = docstring.replace('"""', '\\"\\"\\"').strip()
        lines = safe_docstring.splitlines()
        if len(lines) == 1:
            return f'{indent}"""{lines[0]}"""'
        inner = "\n".join(f"{indent}{line}" if line else "" for line in lines)
        return f'{indent}"""\n{inner}\n{indent}"""'

    def _clean_generated_block(self, value: str) -> str:
        cleaned = textwrap.dedent(value).strip()
        if cleaned.startswith("```"):
            cleaned = (
                cleaned.removeprefix("```python")
                .removeprefix("```")
                .removesuffix("```")
                .strip()
            )
        return cleaned
