# ADR-0004: Shared feature module over Feature Store API

- Status: accepted
- Date: 2026-07-10

## Context

Training (02) and scoring (03) must compute identical features. Options:
duplicate the logic per notebook, use the Databricks Feature Engineering
(Feature Store) API, or share one plain-PySpark module.

## Decision

One module, `notebooks/_features.py`, included via `%run` by both notebooks;
plus one persisted feature snapshot table (`ml_merchant_risk`) computed on the
training window only.

## Rationale

1. **Training/serving skew is the failure mode that matters**; a single code
   path eliminates it by construction. Duplication was never acceptable.
2. The Feature Store API adds concepts (feature tables, lookups, feature specs)
   worth learning, but obscures the *why* — point-in-time correctness — behind
   API calls. For a learning-focused showcase, explicit window functions with
   leakage comments teach more.
3. The merchant-risk snapshot table demonstrates the offline-feature-table
   pattern anyway: computed once on training data, reused verbatim at scoring
   time, because recomputing it on scoring data would leak labels.

## Consequences

- No automatic feature lineage/discovery UI; features are documented in code
  and `architecture.md`.
- Migrating to the Feature Engineering API later is mechanical: each function
  becomes a feature table definition.
