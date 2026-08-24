# ADR 0002 — Quarantine bad rows instead of failing the batch

**Status:** Accepted · **Date:** 2026-08

## Context

A batch of 6,000 orders contains 200 rows violating a quality rule. Options:
fail the whole batch, silently drop the rows, or set them aside.

## Decision

Route failing rows to `ops.quarantine` with the rule names, run id and raw payload.
Reserve hard failure for rules that would corrupt downstream state — a NULL primary
key breaks MERGE idempotency, so that one is `fail`.

## Consequences

**Positive.** 5,800 good rows still reach the business. The bad rows are queryable,
so root cause is a `GROUP BY` rather than an investigation, and they can be replayed
once the upstream bug is fixed. Silently dropping them would make revenue quietly
wrong — the worst outcome of the three.

**Negative.** Downstream consumers must accept that a layer may be *incomplete*
rather than *wrong*. This is made explicit through the DQ pass rate published per
run in `ops.dq_results`.
