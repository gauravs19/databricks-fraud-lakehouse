# ADR-0001: Build a synthetic transaction generator instead of using a public dataset

- Status: accepted
- Date: 2026-07-10

## Context

The project needs financial transaction data with fraud labels. Options:
public datasets (Kaggle PaySim, IEEE-CIS, ULB credit card) or a from-scratch
synthetic generator.

## Decision

Build a seeded synthetic generator (`notebooks/01_generate_transactions.py`)
that writes daily JSONL files with three injected fraud patterns and deliberate
data-quality defects.

## Rationale

1. **Incremental arrival is the point.** Auto Loader, checkpointing and the
   daily job only demonstrate anything if files genuinely arrive over time.
   Static Kaggle CSVs make the ingestion story fake.
2. **Self-contained**: no Kaggle account, license questions, or manual upload
   step; anyone can reproduce the project from the repo alone.
3. **Labels with known ground truth**: fraud patterns are injected by
   construction, so model behavior is explainable (did it catch card-testing
   bursts?) rather than opaque anonymized PCA features (ULB) or unlabeled noise.
4. **DQ defects on demand**: duplicates, nulls, and malformed amounts are
   injected deliberately so Silver expectations and the quarantine pattern have
   real work — public datasets are usually pre-cleaned.
5. Seeded per-day RNG keeps customer profiles stable across incremental runs
   and makes any single day reproducible.

## Consequences

- Fraud patterns are simpler than real adversarial behavior; the model's
  metrics will look better than production reality. Documented in README.
- Generator code is a maintained asset (~200 lines).
- Anyone wanting realism can later swap in IEEE-CIS data behind the same Bronze
  contract without touching Silver or downstream.
