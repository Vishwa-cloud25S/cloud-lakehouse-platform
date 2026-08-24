# ADR 0003 — SCD Type 2 for the customer dimension

**Status:** Accepted · **Date:** 2026-08

## Context

Customers change segment, city and country. A Type-1 dimension overwrites, so
historical revenue is re-attributed to the customer's *current* segment — last
quarter's "Enterprise revenue" changes every time somebody is reclassified.

## Decision

`silver_customers` and `gold.dim_customer` are SCD Type 2: `effective_from`,
`effective_to`, `is_current`, with change detection via `row_hash`.
`fact_orders` carries `customer_version_key` and joins the version whose validity
window contains the order timestamp.

## Consequences

**Positive.** Point-in-time correct reporting. A restated prior quarter is now a
deliberate act, not a side effect of a CRM edit.

**Negative.** The dimension grows with change frequency, and every fact join needs
the band predicate. Both are handled: the dimension is broadcast (it is small), and
`customer_key` is retained alongside `customer_version_key` for consumers that want
current-state analysis.

Products use Type 1 deliberately — nobody asks for historically-correct product
descriptions.
