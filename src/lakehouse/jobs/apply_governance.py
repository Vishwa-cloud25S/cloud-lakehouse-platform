"""Apply the declarative Unity Catalog governance model.

Idempotent: safe to run on every deployment, which is exactly how catalog state
stays reproducible from source control.

    python -m lakehouse.jobs.apply_governance --env dev
"""

from __future__ import annotations

import sys

from lakehouse.governance.masking import MaskingPolicy
from lakehouse.jobs._common import base_parser, build_context, finalise
from lakehouse.logging_utils import get_logger

log = get_logger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = base_parser(__doc__ or "Apply governance")
    args = parser.parse_args(argv)
    ctx = build_context("apply_governance", args.env)
    try:
        model = ctx.catalog.apply_governance_file()

        masking = MaskingPolicy(ctx.spark, catalog=ctx.cfg.catalog)
        masking.create_functions()
        for table, spec in (model.get("tables") or {}).items():
            full = f"{ctx.cfg.catalog}.{table}"
            for column, function in (spec.get("masks") or {}).items():
                masking.attach(full, column, f"{ctx.cfg.catalog}.{function}")
        for table, spec in (model.get("row_filters") or {}).items():
            masking.attach_row_filter(
                f"{ctx.cfg.catalog}.{table}",
                f"{ctx.cfg.catalog}.{spec['function']}",
                spec["columns"],
            )
        log.info("governance.complete", extra={"env": args.env})
        return 0
    finally:
        finalise(ctx)


if __name__ == "__main__":
    sys.exit(main())
