-- =============================================================================
-- Fraud Ops Dashboard — Databricks SQL
-- Create each query in the SQL editor, then assemble into a dashboard with
-- four sections: Executive KPIs / Fraud Operations / Data Quality / Model Health
-- =============================================================================

-- 1. [Exec] Daily volumes and fraud rate ------------------------------------
-- Viz: combo chart — bars: txn_count, line: fraud_rate_pct
SELECT txn_date, txn_count, total_amount, fraud_count, fraud_amount, fraud_rate_pct
FROM workspace.fraud_lakehouse.gold_daily_kpis
ORDER BY txn_date;

-- 2. [Exec] Headline counters (last full day) --------------------------------
-- Viz: counter tiles
SELECT txn_count, fraud_count, fraud_amount, active_customers
FROM workspace.fraud_lakehouse.gold_daily_kpis
ORDER BY txn_date DESC
LIMIT 1;

-- 3. [Fraud Ops] Review queue — today's open alerts ---------------------------
-- Viz: table. This is the analyst work list.
SELECT p.transaction_id, p.event_ts, p.customer_id, p.amount,
       ROUND(p.fraud_score, 3) AS fraud_score,
       s.merchant_id, s.merchant_category, s.country, s.channel
FROM workspace.fraud_lakehouse.ml_predictions p
JOIN workspace.fraud_lakehouse.silver_transactions s USING (transaction_id)
WHERE p.is_alert = 1
  AND DATE(p.event_ts) = (SELECT MAX(DATE(event_ts))
                          FROM workspace.fraud_lakehouse.ml_predictions)
ORDER BY p.fraud_score DESC;

-- 4. [Fraud Ops] Fraud incidence by merchant category ------------------------
-- Viz: horizontal bar
SELECT merchant_category,
       SUM(fraud_count) AS fraud_count,
       ROUND(SUM(fraud_count) * 100.0 / SUM(txn_count), 3) AS fraud_rate_pct
FROM workspace.fraud_lakehouse.gold_merchant_stats
GROUP BY merchant_category
ORDER BY fraud_rate_pct DESC;

-- 5. [Fraud Ops] Highest-risk merchants (min volume filter) ------------------
-- Viz: table
SELECT merchant_id, merchant_category, txn_count, avg_amount, fraud_count, fraud_rate_pct
FROM workspace.fraud_lakehouse.gold_merchant_stats
WHERE txn_count >= 50
ORDER BY fraud_rate_pct DESC
LIMIT 20;

-- 6. [Fraud Ops] Fraud pattern mix over time ----------------------------------
-- Viz: stacked bar by fraud_type
SELECT DATE(event_ts) AS txn_date, fraud_type, COUNT(*) AS fraud_txns
FROM workspace.fraud_lakehouse.silver_transactions
WHERE is_fraud = 1
GROUP BY DATE(event_ts), fraud_type
ORDER BY txn_date;

-- 7. [Fraud Ops] Hour-of-day fraud heatmap ------------------------------------
-- Viz: heatmap (x: hour, y: channel, value: fraud_rate)
SELECT HOUR(event_ts) AS hour_of_day, channel,
       COUNT(*) AS txns,
       ROUND(AVG(is_fraud) * 100, 3) AS fraud_rate_pct
FROM workspace.fraud_lakehouse.silver_transactions
GROUP BY HOUR(event_ts), channel
ORDER BY hour_of_day;

-- 8. [Data Quality] Quarantine trend -------------------------------------------
-- Viz: line — rows rejected by Silver hard expectations per day, by reason
SELECT DATE(_ingested_at) AS ingest_date, _quarantine_reason, COUNT(*) AS rejected_rows
FROM workspace.fraud_lakehouse.silver_transactions_quarantine
GROUP BY DATE(_ingested_at), _quarantine_reason
ORDER BY ingest_date;

-- 9. [Data Quality] Bronze vs Silver reconciliation ----------------------------
-- Viz: counter/table — quantifies total loss through the quality gate
SELECT
  (SELECT COUNT(*) FROM workspace.fraud_lakehouse.bronze_transactions)  AS bronze_rows,
  (SELECT COUNT(*) FROM workspace.fraud_lakehouse.silver_transactions)  AS silver_rows,
  (SELECT COUNT(*) FROM workspace.fraud_lakehouse.silver_transactions_quarantine) AS quarantined_rows;

-- 10. [Model Health] Drift and effectiveness trend ------------------------------
-- Viz: line chart — psi, alert_precision, fraud_recall over time
SELECT monitor_date, model_version, psi, alert_count,
       ROUND(alert_precision, 3) AS alert_precision,
       ROUND(fraud_recall, 3) AS fraud_recall
FROM workspace.fraud_lakehouse.ml_monitoring_metrics
ORDER BY monitor_date;

-- 11. [Model Health] Score distribution (latest day) ----------------------------
-- Viz: bar — compare shape against training baseline by eye
SELECT ROUND(FLOOR(fraud_score * 10) / 10, 1) AS score_bucket, COUNT(*) AS txns
FROM workspace.fraud_lakehouse.ml_predictions
WHERE DATE(event_ts) = (SELECT MAX(DATE(event_ts))
                        FROM workspace.fraud_lakehouse.ml_predictions)
GROUP BY 1
ORDER BY 1;

-- 12. [Model Health] Missed fraud (false negatives) — feedback for next retrain --
-- Viz: table
SELECT s.transaction_id, s.event_ts, s.customer_id, s.amount, s.fraud_type,
       ROUND(p.fraud_score, 3) AS fraud_score
FROM workspace.fraud_lakehouse.silver_transactions s
JOIN workspace.fraud_lakehouse.ml_predictions p USING (transaction_id)
WHERE s.is_fraud = 1 AND p.is_alert = 0
ORDER BY s.event_ts DESC
LIMIT 50;
