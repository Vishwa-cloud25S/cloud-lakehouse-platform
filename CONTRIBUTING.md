# Contributing

## Setup

```bash
pip install -r requirements-dev.txt
pre-commit install
make seed
make demo
```

Requires Python 3.10+ and a JDK (11 or 17).

## Before opening a PR

```bash
make lint       # ruff + black
make typecheck  # mypy
make test       # unit tests
make test-all   # + integration tests
```

## Adding a dataset

1. Add a block to `conf/datasets.yaml`
2. Add `conf/expectations/<dataset>.yaml`
3. If it needs bespoke logic, add a conformer to `transforms/silver.py` and register
   it in `CONFORMERS`
4. Add the dataset to the Terraform job task list

No changes to the job modules should be required.

## Adding a quality rule

Edit the suite YAML. Choose the action deliberately:

- `fail` only if the rule protects downstream integrity
- `quarantine` for recoverable row-level problems
- `warn` (with a tolerance) for soft signals

## Testing philosophy

Unit tests cover pure logic and Spark transforms. Integration tests run against a
real local Delta lake and assert the guarantees that matter: idempotency,
incrementality, schema evolution and SCD2 history.

If you fix a bug, add the test that would have caught it.

## Commit style

Conventional Commits: `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`.
