# ADR 0004 — Datasets declared in YAML, not Python

**Status:** Accepted · **Date:** 2026-08

## Context

Each new source could be a new module implementing an interface, or an entry in a
declarative contract consumed by generic jobs.

## Decision

`conf/datasets.yaml` declares keys, watermark, partitioning, Z-ORDER, SCD type,
evolution policy and quality suite. The jobs take only `--env` and `--dataset`.

## Consequences

**Positive.** Onboarding a dataset is a reviewable config diff, and the same code
path is exercised by every dataset — a bug fixed once is fixed everywhere.
Promoting dev → prod never edits Python.

**Negative.** A genuinely unusual source cannot express itself in the contract. The
escape hatch is `transforms.silver.CONFORMERS`, a registry mapping a dataset name to
a bespoke function; the generic path still handles ingestion, quality and writing.
