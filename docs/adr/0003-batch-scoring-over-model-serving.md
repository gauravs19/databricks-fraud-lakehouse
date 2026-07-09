# ADR-0003: Batch scoring instead of a real-time Model Serving endpoint

- Status: accepted
- Date: 2026-07-10

## Context

Fraud models can be deployed as always-on REST endpoints (authorization-time
scoring) or as scheduled batch inference (detection/review scoring). Free
Edition limits Model Serving; batch is fully supported.

## Decision

Daily batch scoring (`notebooks/03_batch_score.py`): load `@champion` from the
UC registry, score the latest day, MERGE into `ml_predictions`.

## Rationale

1. **Free-tier constraint**: an always-on serving endpoint isn't a realistic
   free-tier workload; batch inference on serverless is.
2. **It is also the honest pattern**: real card-fraud stacks split into
   sub-second *authorization* scoring (online feature store + serving, outside
   lakehouse latency) and batch *detection* scoring feeding analyst review
   queues and retraining. This project implements the second layer and
   documents the first (`architecture.md`).
3. Batch scoring showcases the more architecturally interesting properties:
   idempotent MERGE, alias-based rollback, per-row model lineage.

## Consequences

- No live endpoint to demo; the "demo surface" is the dashboard review queue.
- If Free Edition serving quotas allow later, a serving endpoint over the same
  registered model is an additive notebook, not a redesign.
