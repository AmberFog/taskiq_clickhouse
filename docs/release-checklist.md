# Local release checklist

This checklist verifies the source tree and package artifact. It does not
create a tag, push a branch, publish to PyPI, or create a GitHub release.

Run every command from the repository root with Docker available.

## 1. Source and service checks

```shell
docker run --rm -v "$PWD:/repo:ro" -w /repo \
  rhysd/actionlint:1.7.12@sha256:b1934ee5f1c509618f2508e6eb47ee0d3520686341fec936f3b79331f9315667 \
  -color
uv lock --check
uv run pre-commit run --all-files
git ls-files --cached --others --exclude-standard -z | xargs -0 uv run pre-commit run --files
uv run pytest --cov=taskiq_clickhouse --cov-report=term-missing
```

GitHub Actions owns the Python, Taskiq, Pydantic, ClickHouse-server, and
clickhouse-connect compatibility matrices directly.

## 2. Build and clean-install the package

```shell
mkdir -p "$PWD/.test-artifacts"
PACKAGE_WORKDIR="$(mktemp -d "$PWD/.test-artifacts/package-check.XXXXXX")"
uv build --no-sources --out-dir "$PACKAGE_WORKDIR/dist"

VERSION="$(uv version --short)"
WHEEL="$PACKAGE_WORKDIR/dist/taskiq_clickhouse-${VERSION}-py3-none-any.whl"
SDIST="$PACKAGE_WORKDIR/dist/taskiq_clickhouse-${VERSION}.tar.gz"
test -f "$WHEEL"
test -f "$SDIST"

uv venv --python "$(cat .python-version)" "$PACKAGE_WORKDIR/.venv"
uv pip install --python "$PACKAGE_WORKDIR/.venv/bin/python" "$WHEEL"
cd "$PACKAGE_WORKDIR"
"$PACKAGE_WORKDIR/.venv/bin/python" -I -c \
  'from importlib import metadata; import taskiq_clickhouse; assert metadata.version("taskiq-clickhouse")'
"$PACKAGE_WORKDIR/.venv/bin/taskiq-clickhouse-schema" --help
cd -
```

The isolated import and console command catch missing package files, broken
metadata, and missing entry points without maintaining a second test framework
for the wheel.

## 3. External CI gate

After the source is committed, require the GitHub Actions `Required gate` to
pass. Review the native compatibility and ClickHouse matrix cells individually.
Do not publish merely because the checks passed; publication remains a separate
owner-authorized task.
