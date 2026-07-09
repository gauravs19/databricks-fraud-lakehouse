# ADR-0002: Lakeflow Declarative Pipelines (DLT) for the medallion, not notebook jobs

- Status: accepted
- Date: 2026-07-10

## Context

Bronze→Silver→Gold could be implemented as (a) chained notebooks with manual
`readStream`/`writeStream` and checkpoint management, (b) plain SQL jobs, or
(c) a Lakeflow Declarative Pipeline.

## Decision

Lakeflow Declarative Pipeline (`pipelines/fraud_pipeline.py`), triggered mode,
serverless.

## Rationale

1. **Declared dependencies**: the framework derives the DAG from
   `dlt.read`/`read_stream` references — no hand-maintained task ordering
   between medallion layers.
2. **Expectations as first-class data contracts**: hard/soft rules live next to
   the table definition, and their pass/fail metrics land in the event log
   automatically — the DQ dashboard costs nothing extra.
3. **Managed checkpoints and full-refresh semantics** replace the most
   error-prone part of hand-rolled structured streaming.
4. It is the pattern Databricks positions for production ETL; demonstrating it
   is part of the showcase value.

## Consequences

- Framework lock-in for the pipeline file (mitigated: transformations are
  plain PySpark, portable by extraction).
- Gold tables are materialized views recomputed by the pipeline — fine at this
  scale; at real scale some would become incremental streaming aggregations.
- Quarantine is implemented as an inverse-filtered table because expectations
  drop rows but don't route them; this is the standard community pattern.
