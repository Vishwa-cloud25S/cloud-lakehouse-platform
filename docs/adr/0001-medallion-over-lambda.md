# ADR 0001 — Medallion architecture over a Lambda architecture

**Status:** Accepted · **Date:** 2026-08

## Context

The platform must serve both a daily BI refresh and, eventually, lower-latency
consumers. The classic answer is Lambda: a batch path and a speed path, reconciled
at query time.

## Decision

Use a single medallion (Bronze/Silver/Gold) architecture on Delta Lake. Latency is
reduced by shortening the batch interval and using Auto Loader / Structured
Streaming against the *same* tables, not by adding a parallel path.

## Consequences

**Positive.** One implementation of every business rule. Lambda's real cost is not
infrastructure but the two copies of "what does net revenue mean" that inevitably
drift. Delta's ACID guarantees make concurrent streaming writes and batch reads
safe on one table, which is precisely what removes the need for a speed layer.

**Negative.** True sub-second serving would still require a separate store. That is
an accepted limit: the requirement here is minutes, not milliseconds.
