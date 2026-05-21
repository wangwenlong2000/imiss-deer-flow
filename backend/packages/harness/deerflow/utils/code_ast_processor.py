"""AST-based helpers for Python code extraction, normalization, and validation."""

from __future__ import annotations

import ast
import builtins
import textwrap
from collections.abc import Iterable
from typing import TypedDict


class IOExtractionResult(TypedDict):
    input: list[str]
    output: list[str]


class ValidationResult(TypedDict):
    valid: bool
    error: str


class _NameUsageVisitor(ast.NodeVisitor):
    """收集代码中的名称读写关系，用于区分输入变量与本地绑定变量。"""

    def __init__(self) -> None:
        self.loaded_names: list[str] = []
        self.bound_names: set[str] = set()
        self.outputs: list[str] = []
        self.return_outputs: list[str] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.bound_names.add(node.name)

        for arg in self._iter_arguments(node.args):
            self.bound_names.add(arg.arg)

        for default in [*node.args.defaults, *node.args.kw_defaults]:
            if default is not None:
                self.visit(default)

        for stmt in node.body:
            self.visit(stmt)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.visit_FunctionDef(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.bound_names.add(node.name)

        for base in node.bases:
            self.visit(base)
        for keyword in node.keywords:
            self.visit(keyword)

        # 类体会形成独立命名空间；这里不把类内部方法的局部变量混入外层分析。
        for stmt in node.body:
            if not isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                self.visit(stmt)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self.bound_names.add(alias.asname or alias.name.split(".", maxsplit=1)[0])

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for alias in node.names:
            self.bound_names.add(alias.asname or alias.name)

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Load):
            self.loaded_names.append(node.id)
        elif isinstance(node.ctx, (ast.Store, ast.Del)):
            self.bound_names.add(node.id)

    def visit_Assign(self, node: ast.Assign) -> None:
        self.visit(node.value)
        targets = self._names_from_targets(node.targets)
        self.bound_names.update(targets)
        self.outputs = targets

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        self.visit(node.annotation)
        if node.value is not None:
            self.visit(node.value)
        targets = self._names_from_targets([node.target])
        self.bound_names.update(targets)
        self.outputs = targets

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        # 增量赋值既读取旧值，又写回目标，例如 total += value。
        self._visit_target_as_load(node.target)
        self.visit(node.value)
        targets = self._names_from_targets([node.target])
        self.bound_names.update(targets)
        self.outputs = targets

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        self.visit(node.value)
        targets = self._names_from_targets([node.target])
        self.bound_names.update(targets)
        self.outputs = targets

    def visit_For(self, node: ast.For) -> None:
        self.visit(node.iter)
        self.bound_names.update(self._names_from_targets([node.target]))
        for stmt in [*node.body, *node.orelse]:
            self.visit(stmt)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> None:
        self.visit_For(node)

    def visit_With(self, node: ast.With) -> None:
        for item in node.items:
            self.visit(item.context_expr)
            if item.optional_vars is not None:
                self.bound_names.update(self._names_from_targets([item.optional_vars]))
        for stmt in node.body:
            self.visit(stmt)

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:
        self.visit_With(node)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if node.type is not None:
            self.visit(node.type)
        if node.name is not None:
            self.bound_names.add(node.name)
        for stmt in node.body:
            self.visit(stmt)

    def visit_Return(self, node: ast.Return) -> None:
        if node.value is not None:
            self.visit(node.value)
            self.return_outputs = self._names_from_expr(node.value)

    def visit_ListComp(self, node: ast.ListComp) -> None:
        self._visit_comprehension(node.generators, node.elt)

    def visit_SetComp(self, node: ast.SetComp) -> None:
        self._visit_comprehension(node.generators, node.elt)

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
        self._visit_comprehension(node.generators, node.elt)

    def visit_DictComp(self, node: ast.DictComp) -> None:
        for generator in node.generators:
            self.visit(generator.iter)
            self.bound_names.update(self._names_from_targets([generator.target]))
            for condition in generator.ifs:
                self.visit(condition)
        self.visit(node.key)
        self.visit(node.value)

    @staticmethod
    def _iter_arguments(args: ast.arguments) -> Iterable[ast.arg]:
        yield from args.posonlyargs
        yield from args.args
        if args.vararg is not None:
            yield args.vararg
        yield from args.kwonlyargs
        if args.kwarg is not None:
            yield args.kwarg

    def _visit_comprehension(self, generators: list[ast.comprehension], element: ast.AST) -> None:
        # 推导式的目标变量是局部绑定，先访问迭代来源，再记录绑定目标。
        for generator in generators:
            self.visit(generator.iter)
            self.bound_names.update(self._names_from_targets([generator.target]))
            for condition in generator.ifs:
                self.visit(condition)
        self.visit(element)

    def _visit_target_as_load(self, target: ast.AST) -> None:
        if isinstance(target, ast.Name):
            self.loaded_names.append(target.id)
        else:
            self.visit(target)

    def _names_from_targets(self, targets: Iterable[ast.AST]) -> list[str]:
        names: list[str] = []
        for target in targets:
            for child in ast.walk(target):
                if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Store):
                    names.append(child.id)
        return _unique(names)

    def _names_from_expr(self, expr: ast.AST) -> list[str]:
        if isinstance(expr, ast.Name):
            return [expr.id]
        if isinstance(expr, (ast.Tuple, ast.List)):
            names: list[str] = []
            for element in expr.elts:
                names.extend(self._names_from_expr(element))
            return _unique(names)
        return []


class CodeASTProcessor:
    """Process Python source code with the standard :mod:`ast` module only."""

    _BUILTIN_NAMES: set[str] = set(dir(builtins))

    def extract_io(self, code: str) -> IOExtractionResult:
        """Extract input and output variable names from a function or code snippet."""

        try:
            tree = ast.parse(textwrap.dedent(code).strip())
        except SyntaxError:
            return {"input": [], "output": []}

        visitor = _NameUsageVisitor()
        visitor.visit(tree)

        inputs = [
            name
            for name in _unique(visitor.loaded_names)
            if name not in visitor.bound_names and name not in self._BUILTIN_NAMES
        ]

        function_args = self._function_argument_names(tree)
        inputs = _unique([*function_args, *inputs])
        outputs = visitor.return_outputs or visitor.outputs

        return {"input": inputs, "output": outputs}

    def normalize_code(self, code: str, function_name: str = "generated_function") -> str:
        """Wrap a code snippet into a standard Python function definition."""

        source = textwrap.dedent(code).strip()
        if not source:
            raise ValueError("code must not be empty")

        try:
            tree = ast.parse(source)
        except SyntaxError as exc:
            raise ValueError(f"invalid Python code: {exc.msg}") from exc

        if self._has_top_level_definition(tree):
            return ast.unparse(tree)

        io_result = self.extract_io(source)
        args = ", ".join(io_result["input"])
        body = list(tree.body)

        if body and isinstance(body[-1], ast.Expr):
            body[-1] = ast.Return(value=body[-1].value)
        elif io_result["output"]:
            body.append(ast.Return(value=self._build_return_value(io_result["output"])))

        function = ast.FunctionDef(
            name=function_name,
            args=ast.arguments(
                posonlyargs=[],
                args=[ast.arg(arg=name) for name in io_result["input"]],
                vararg=None,
                kwonlyargs=[],
                kw_defaults=[],
                kwarg=None,
                defaults=[],
            ),
            body=body or [ast.Pass()],
            decorator_list=[],
            returns=None,
            type_params=[],
        )
        module = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))

        rendered = ast.unparse(module)
        if args and f"def {function_name}()" in rendered:
            rendered = rendered.replace(f"def {function_name}()", f"def {function_name}({args})", 1)
        return rendered

    def validate_format(self, code: str) -> ValidationResult:
        """Validate syntax and require at least one function or class definition."""

        try:
            tree = ast.parse(textwrap.dedent(code).strip())
        except SyntaxError as exc:
            return {"valid": False, "error": f"SyntaxError: {exc.msg} at line {exc.lineno}"}

        has_definition = any(
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            for node in tree.body
        )
        if not has_definition:
            return {"valid": False, "error": "代码必须包含至少一个完整的函数定义或类定义"}

        return {"valid": True, "error": ""}

    def _function_argument_names(self, tree: ast.Module) -> list[str]:
        for stmt in tree.body:
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return [arg.arg for arg in _NameUsageVisitor._iter_arguments(stmt.args)]
        return []

    @staticmethod
    def _has_top_level_definition(tree: ast.Module) -> bool:
        return any(
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            for node in tree.body
        )

    @staticmethod
    def _build_return_value(names: list[str]) -> ast.expr:
        if len(names) == 1:
            return ast.Name(id=names[0], ctx=ast.Load())
        return ast.Tuple(elts=[ast.Name(id=name, ctx=ast.Load()) for name in names], ctx=ast.Load())


def _unique(names: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for name in names:
        if name not in seen:
            seen.add(name)
            result.append(name)
    return result
