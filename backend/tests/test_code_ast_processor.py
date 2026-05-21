"""Tests for AST-based Python code processing."""

import ast

from deerflow.utils.code_ast_processor import CodeASTProcessor


def test_extract_io_from_function() -> None:
    code = """
    def build_total(price: float, tax_rate: float) -> float:
        subtotal = price * quantity
        total = subtotal * (1 + tax_rate)
        return total
    """

    result = CodeASTProcessor().extract_io(code)

    assert result == {"input": ["price", "tax_rate", "quantity"], "output": ["total"]}


def test_extract_io_from_snippet_assignment() -> None:
    result = CodeASTProcessor().extract_io("res = requests.get(url).json()")

    assert result == {"input": ["requests", "url"], "output": ["res"]}


def test_normalize_code_wraps_snippet_into_function() -> None:
    normalized = CodeASTProcessor().normalize_code(
        "res = requests.get(url).json()",
        function_name="fetch_json",
    )

    parsed = ast.parse(normalized)
    function = parsed.body[0]

    assert isinstance(function, ast.FunctionDef)
    assert function.name == "fetch_json"
    assert [arg.arg for arg in function.args.args] == ["requests", "url"]
    assert isinstance(function.body[-1], ast.Return)


def test_normalize_code_keeps_existing_definition() -> None:
    code = """
    def existing(value: int) -> int:
        return value + 1
    """

    normalized = CodeASTProcessor().normalize_code(code)

    assert "def existing(value: int) -> int:" in normalized


def test_validate_format_accepts_function_or_class() -> None:
    processor = CodeASTProcessor()

    assert processor.validate_format("def ok():\n    pass") == {"valid": True, "error": ""}
    assert processor.validate_format("class Box:\n    pass") == {"valid": True, "error": ""}


def test_validate_format_rejects_invalid_or_incomplete_code() -> None:
    processor = CodeASTProcessor()

    syntax_result = processor.validate_format("def broken(:\n    pass")
    assert syntax_result["valid"] is False
    assert "SyntaxError" in syntax_result["error"]

    shape_result = processor.validate_format("answer = 42")
    assert shape_result == {"valid": False, "error": "代码必须包含至少一个完整的函数定义或类定义"}
