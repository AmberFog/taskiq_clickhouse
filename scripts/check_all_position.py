"""Validate that module export declarations stay at the file top."""

import ast
from pathlib import Path
import sys


def main() -> int:
    """Check every Python path supplied by pre-commit."""
    errors: list[str] = []
    for argument in sys.argv[1:]:
        path = Path(argument)
        if not path.exists() or path.suffix != ".py":
            continue
        errors.extend(check_file(path))

    if errors:
        sys.stderr.write("\n".join(errors) + "\n")
        return 1
    return 0


def check_file(path: Path) -> list[str]:
    """Return an error when ``__all__`` follows imports or definitions."""
    module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    all_index = all_assignment_index(module)
    if all_index is None:
        return []

    expected_index = first_export_index(module)
    if all_index == expected_index:
        return []

    return [f"{path}: move __all__ to the file top"]


def first_export_index(module: ast.Module) -> int:
    """Find the first legal export position after docs and future imports."""
    statement_index = 1 if ast.get_docstring(module) else 0
    while statement_index < len(module.body):
        statement = module.body[statement_index]
        if not isinstance(statement, ast.ImportFrom) or statement.module != "__future__":
            break
        statement_index += 1
    return statement_index


def all_assignment_index(module: ast.Module) -> int | None:
    """Find the first module-level ``__all__`` assignment."""
    for statement_index, statement in enumerate(module.body):
        if is_all_assignment(statement):
            return statement_index
    return None


def is_all_assignment(statement: ast.stmt) -> bool:
    """Return whether an AST statement assigns ``__all__``."""
    if isinstance(statement, ast.Assign):
        return any(isinstance(target, ast.Name) and target.id == "__all__" for target in statement.targets)
    if isinstance(statement, ast.AnnAssign):
        target = statement.target
        return isinstance(target, ast.Name) and target.id == "__all__"
    return False


if __name__ == "__main__":
    raise SystemExit(main())
