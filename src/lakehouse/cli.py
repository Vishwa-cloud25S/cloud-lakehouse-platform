"""Unified CLI: `lakehouse <command> [options]`."""

from __future__ import annotations

import argparse
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lakehouse", description="cloud-lakehouse-platform command line"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    for name, help_text in (
        ("bronze", "incremental ingest into Bronze"),
        ("silver", "build Silver with quality enforcement"),
        ("gold", "build the Gold star schema"),
        ("maintenance", "OPTIMIZE / VACUUM / ANALYZE"),
        ("governance", "apply the Unity Catalog governance model"),
    ):
        p = sub.add_parser(name, help=help_text)
        p.add_argument("--env", default="dev")
        p.add_argument("--dataset", default="orders")
        p.add_argument("--full-refresh", action="store_true")
        p.add_argument("--dry-run", action="store_true")

    args = parser.parse_args(argv)
    passthrough = ["--env", args.env, "--dataset", args.dataset]
    if args.full_refresh:
        passthrough.append("--full-refresh")
    if args.dry_run:
        passthrough.append("--dry-run")

    if args.command == "bronze":
        from lakehouse.jobs.ingest_bronze import main as run
    elif args.command == "silver":
        from lakehouse.jobs.build_silver import main as run
    elif args.command == "gold":
        from lakehouse.jobs.build_gold import main as run
    elif args.command == "maintenance":
        from lakehouse.jobs.run_maintenance import main as run
    else:
        from lakehouse.jobs.apply_governance import main as run

    return run(passthrough)


if __name__ == "__main__":
    sys.exit(main())
