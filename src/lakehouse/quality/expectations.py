"""Expectation model.

An expectation is a named boolean SQL predicate over a row plus an action:

* ``warn``      - record the failure, let the row through
* ``quarantine``- route failing rows to the quarantine table, continue
* ``fail``      - abort the job (use for anything that would corrupt Gold)

Suites live in `conf/expectations/<name>.yaml`, so an analyst can add a rule via
pull request without touching Spark code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import yaml

from lakehouse.config import CONF_DIR


class Action(str, Enum):
    WARN = "warn"
    QUARANTINE = "quarantine"
    FAIL = "fail"


@dataclass(frozen=True)
class Expectation:
    """One named rule: rows satisfying `expr` pass."""

    name: str
    expr: str
    action: Action = Action.QUARANTINE
    description: str = ""
    # Allow a small fraction of failures before the action triggers (0.0 = zero tolerance).
    tolerance: float = 0.0

    @staticmethod
    def from_dict(name: str, data: dict[str, Any]) -> Expectation:
        return Expectation(
            name=name,
            expr=data["expr"],
            action=Action(data.get("action", "quarantine")),
            description=data.get("description", ""),
            tolerance=float(data.get("tolerance", 0.0)),
        )


@dataclass(frozen=True)
class ExpectationSuite:
    """All rules for one dataset."""

    name: str
    expectations: list[Expectation] = field(default_factory=list)
    dataset: str = ""

    @property
    def blocking(self) -> list[Expectation]:
        return [e for e in self.expectations if e.action is Action.FAIL]

    @property
    def quarantining(self) -> list[Expectation]:
        return [e for e in self.expectations if e.action is Action.QUARANTINE]

    def pass_expression(self, actions: tuple[Action, ...]) -> str:
        """Combined SQL predicate that is TRUE only when all listed rules pass."""
        exprs = [f"({e.expr})" for e in self.expectations if e.action in actions]
        return " AND ".join(exprs) if exprs else "true"


def load_suite(name: str, conf_dir: Path | None = None) -> ExpectationSuite:
    """Load `conf/expectations/<name>.yaml`."""
    base = conf_dir or (CONF_DIR / "expectations")
    path = Path(base) / f"{name}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"expectation suite not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return ExpectationSuite(
        name=data.get("suite", name),
        dataset=data.get("dataset", name),
        expectations=[
            Expectation.from_dict(rule_name, spec)
            for rule_name, spec in (data.get("expectations") or {}).items()
        ],
    )
