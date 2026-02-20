"""Evaluate serving predictions and trigger retraining when metrics degrade.

This DAG computes two metrics from `prediction_request_log` rows that have labels:
  - Accuracy (threshold 0.5)
  - Adaptive ECE (ACE) using quantile binning with n_bins=10

If metrics cross configured thresholds, it triggers the training DAG.

Variables (optional):
  - lol_gnn_data_postgres_conn_id: Postgres conn id (default DATA_POSTGRES_CONNECTION)
  - lol_gnn_eval_window_days: evaluation window in days (default 7)
  - lol_gnn_eval_min_samples: minimum labeled samples required (default 1000)
  - lol_gnn_eval_n_bins: ACE bins (default 10)
  - lol_gnn_trigger_acc_min: trigger if accuracy < this (default 0.52)
  - lol_gnn_trigger_ace_max: trigger if ace > this (default 0.08)
  - lol_gnn_retrain_target_dag_id: DAG to trigger (default lol_gnn_train_and_gate)
  - lol_gnn_retrain_conf_json: optional JSON string for dag_run.conf
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import pendulum
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable, dag, task


DEFAULT_PG_CONN_ID = "DATA_POSTGRES_CONNECTION"


def _get_pg_conn_id() -> str:
    return Variable.get("lol_gnn_data_postgres_conn_id", default=DEFAULT_PG_CONN_ID)


def _var_int(key: str, default: int) -> int:
    try:
        return int(Variable.get(key, default=default))
    except Exception:
        return default


def _var_float(key: str, default: float) -> float:
    try:
        return float(Variable.get(key, default=default))
    except Exception:
        return default


def _quantile_bins(values: List[float], n_bins: int) -> List[Tuple[int, int]]:
    """Return list of (start_idx, end_idx) ranges over sorted values."""
    n = len(values)
    if n == 0:
        return []
    n_bins = max(1, min(n_bins, n))
    bins = []
    for b in range(n_bins):
        start = (b * n) // n_bins
        end = ((b + 1) * n) // n_bins
        if end <= start:
            continue
        bins.append((start, end))
    return bins


def compute_accuracy_and_ace(preds: List[float], labels: List[int], n_bins: int = 10) -> Dict[str, float]:
    """Compute accuracy and adaptive ECE (ACE) with quantile bins."""
    n = len(preds)
    if n == 0 or n != len(labels):
        return {"accuracy": float("nan"), "ace": float("nan"), "n": 0}

    # Accuracy @ 0.5
    correct = 0
    for p, y in zip(preds, labels):
        pred_label = 1 if p >= 0.5 else 0
        if pred_label == int(y):
            correct += 1
    acc = correct / n

    # ACE: sort by confidence
    pairs = sorted(zip(preds, labels), key=lambda x: x[0])
    sorted_preds = [p for p, _ in pairs]
    sorted_labels = [int(y) for _, y in pairs]

    ace = 0.0
    for start, end in _quantile_bins(sorted_preds, n_bins):
        bin_preds = sorted_preds[start:end]
        bin_labels = sorted_labels[start:end]
        m = len(bin_preds)
        if m == 0:
            continue
        conf = sum(bin_preds) / m
        bin_acc = sum(bin_labels) / m
        ace += (m / n) * abs(conf - bin_acc)

    return {"accuracy": float(acc), "ace": float(ace), "n": int(n)}


@dag(
    dag_id="lol_evaluate_and_trigger_retrain",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule="0 3 * * *",  # daily 03:00 KST
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["monitoring", "evaluation", "retrain"],
    doc_md=__doc__,
)
def evaluate_and_trigger_retrain_dag():
    @task
    def load_labeled_predictions() -> Dict[str, Any]:
        window_days = _var_int("lol_gnn_eval_window_days", 7)
        min_samples = _var_int("lol_gnn_eval_min_samples", 1000)

        hook = PostgresHook(postgres_conn_id=_get_pg_conn_id())
        sql = """
            SELECT pred_blue_win_prob, actual_blue_win
            FROM prediction_request_log
            WHERE actual_blue_win IS NOT NULL
              AND pred_blue_win_prob IS NOT NULL
              AND created_at >= now() - (%s || ' days')::interval;
        """
        rows = hook.get_records(sql, parameters=(window_days,))

        preds: List[float] = []
        labels: List[int] = []
        for p, y in rows:
            try:
                preds.append(float(p))
                labels.append(1 if bool(y) else 0)
            except Exception:
                continue

        payload = {
            "preds": preds,
            "labels": labels,
            "window_days": window_days,
            "min_samples": min_samples,
        }
        print(f"Loaded labeled samples: n={len(preds)} window_days={window_days}")
        return payload

    @task
    def compute_metrics(data: Dict[str, Any]) -> Dict[str, Any]:
        preds = data.get("preds") or []
        labels = data.get("labels") or []
        window_days = int(data.get("window_days") or 7)
        min_samples = int(data.get("min_samples") or 1000)
        n_bins = _var_int("lol_gnn_eval_n_bins", 10)

        n = min(len(preds), len(labels))
        if n < min_samples:
            metrics = {
                "n": int(n),
                "window_days": window_days,
                "skipped": True,
                "reason": f"not_enough_samples<{min_samples}",
            }
            print(f"Skipping evaluation: {metrics}")
            return metrics

        m = compute_accuracy_and_ace(preds[:n], labels[:n], n_bins=n_bins)
        metrics = {
            **m,
            "window_days": window_days,
            "n_bins": n_bins,
            "skipped": False,
        }
        print(f"Evaluation metrics: {metrics}")
        return metrics

    def _should_trigger(**context) -> bool:
        metrics = context["ti"].xcom_pull(task_ids="compute_metrics") or {}
        if metrics.get("skipped"):
            return False

        acc_min = _var_float("lol_gnn_trigger_acc_min", 0.52)
        ace_max = _var_float("lol_gnn_trigger_ace_max", 0.08)

        acc = float(metrics.get("accuracy", 0.0))
        ace = float(metrics.get("ace", 0.0))

        trigger = (acc < acc_min) or (ace > ace_max)
        print(
            "Trigger decision: "
            f"acc={acc:.4f} (min {acc_min}) ace={ace:.4f} (max {ace_max}) => trigger={trigger}"
        )
        return bool(trigger)

    data = load_labeled_predictions()
    metrics = compute_metrics(data)

    gate = ShortCircuitOperator(
        task_id="decide_trigger",
        python_callable=_should_trigger,
        provide_context=True,
    )

    target_dag_id = Variable.get("lol_gnn_retrain_target_dag_id", default="lol_gnn_train_and_gate")
    conf_raw = Variable.get("lol_gnn_retrain_conf_json", default="")
    conf = {}
    if conf_raw:
        try:
            conf = json.loads(conf_raw)
        except Exception:
            conf = {}

    trigger = TriggerDagRunOperator(
        task_id="trigger_retraining",
        trigger_dag_id=target_dag_id,
        conf=conf,
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # Ordering
    metrics >> gate >> trigger


dag = evaluate_and_trigger_retrain_dag()
