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
  - lol_gnn_retrain_target_dag_id: DAG to trigger (default lol_gnn_build_trainset)
  - lol_gnn_retrain_conf_json: optional JSON string for dag_run.conf
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pendulum
import requests
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable, dag, task


DEFAULT_PG_CONN_ID = "DATA_POSTGRES_CONNECTION"


def _resolve_calendar_path(host_training_dir: str, calendar_file: str) -> Path:
    primary = Path(host_training_dir) / calendar_file
    if primary.exists():
        return primary
    fallback = Path(__file__).resolve().parents[1] / "src" / "training" / calendar_file
    if fallback.exists():
        return fallback
    raise FileNotFoundError(f"Calendar file not found: {primary} (fallback: {fallback})")


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


def _parse_calendar_datetime(raw: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported datetime format: {raw}")


def _is_calendar_offset_day(calendar_obj: Dict[str, Any], now_kst: datetime) -> bool:
    target_date = now_kst.date()
    windows = calendar_obj.get("windows", [])
    if not isinstance(windows, list) or not windows:
        return False

    for window in windows:
        start_raw = window.get("start_date")
        if not start_raw:
            continue
        start_date = _parse_calendar_datetime(str(start_raw)).date()
        if target_date == start_date + timedelta(days=1):
            return True
        if target_date == start_date + timedelta(days=3):
            return True
    return False


def _resolve_current_calendar_start(calendar_obj: Dict[str, Any], now_kst: datetime) -> datetime:
    if calendar_obj.get("start_date"):
        return _parse_calendar_datetime(str(calendar_obj["start_date"]))

    windows = calendar_obj.get("windows", [])
    if not isinstance(windows, list) or not windows:
        raise ValueError("Calendar JSON must include non-empty `windows` list")

    candidates: List[datetime] = []
    for window in windows:
        start_raw = window.get("start_date")
        if not start_raw:
            continue
        start_dt = _parse_calendar_datetime(str(start_raw))
        if start_dt <= now_kst:
            candidates.append(start_dt)

    if not candidates:
        raise ValueError("No calendar window matched current KST datetime")
    return max(candidates)


def _match_id(platform_id: str, game_id: int) -> str:
    return f"{platform_id}_{game_id}"


def _extract_blue_win(match_payload: Dict[str, Any]) -> Optional[bool]:
    info = (match_payload or {}).get("info") or {}

    teams = info.get("teams")
    if isinstance(teams, list):
        for t in teams:
            if isinstance(t, dict) and t.get("teamId") == 100:
                if "win" in t:
                    return bool(t.get("win"))
                if "won" in t:
                    return bool(t.get("won"))
                if "teamWin" in t:
                    return bool(t.get("teamWin"))

    participants = info.get("participants")
    if isinstance(participants, list) and participants:
        for p in participants:
            if isinstance(p, dict) and p.get("teamId") == 100 and "win" in p:
                return bool(p.get("win"))
    return None


def _fetch_match_from_riot(match_id: str, api_key: str) -> Optional[Dict[str, Any]]:
    url = f"https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}"
    headers = {"X-Riot-Token": api_key}
    r = requests.get(url, headers=headers, timeout=(3.0, 15.0))
    if r.status_code == 200:
        return r.json()
    if r.status_code == 404:
        return None
    raise RuntimeError(f"Riot API error {r.status_code}: {r.text[:300]}")


def _backfill_missing_actuals(
    hook: PostgresHook,
    eval_start: datetime,
    limit: int,
    min_age_minutes: int,
    sleep_between_sec: float,
) -> Dict[str, int]:
    sql_pending = """
        SELECT DISTINCT platform_id, game_id
        FROM prediction_request_log
        WHERE actual_blue_win IS NULL
          AND status_code = 200
          AND game_id IS NOT NULL
          AND created_at >= %s
          AND created_at < now() - (%s || ' minutes')::interval
        ORDER BY game_id ASC
        LIMIT %s;
    """
    rows = hook.get_records(sql_pending, parameters=(eval_start, min_age_minutes, limit))
    pendings = [{"platform_id": r[0] or "KR", "game_id": int(r[1])} for r in rows]

    api_key = None
    try:
        api_key = Variable.get("RIOT_API_KEY")
    except Exception:
        api_key = None

    updated = 0
    cache_hits = 0
    cache_misses = 0
    skipped_unknown = 0
    errors = 0

    for item in pendings:
        platform_id = str(item["platform_id"])
        game_id = int(item["game_id"])
        mid = _match_id(platform_id, game_id)

        match_payload = None
        try:
            rec = hook.get_first(
                "SELECT match_json FROM matches WHERE match_id = %s;",
                parameters=(mid,),
            )
            if rec and rec[0] is not None:
                match_payload = rec[0]
                if isinstance(match_payload, str):
                    match_payload = json.loads(match_payload)
                cache_hits += 1
            else:
                cache_misses += 1
        except Exception as e:
            errors += 1
            print(f"[WARN] DB cache lookup failed for {mid}: {e}")

        if match_payload is None:
            if not api_key:
                continue
            try:
                match_payload = _fetch_match_from_riot(mid, api_key)
            except Exception as e:
                errors += 1
                print(f"[WARN] Riot fetch failed for {mid}: {e}")
                continue

        if not match_payload:
            continue

        blue_win = _extract_blue_win(match_payload)
        if blue_win is None:
            skipped_unknown += 1
            continue

        try:
            hook.run(
                """
                UPDATE prediction_request_log
                SET actual_blue_win = %s,
                    actual_fetched_at = now()
                WHERE platform_id = %s
                  AND game_id = %s
                  AND actual_blue_win IS NULL
                  AND created_at >= %s;
                """,
                parameters=(blue_win, platform_id, game_id, eval_start),
            )
            updated += 1
        except Exception as e:
            errors += 1
            print(f"[WARN] Update failed for {platform_id}/{game_id}: {e}")

        if sleep_between_sec:
            time.sleep(sleep_between_sec)

    return {
        "pending": len(pendings),
        "updated": updated,
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "skipped_unknown": skipped_unknown,
        "errors": errors,
    }


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
        backfill_limit = _var_int("lol_gnn_eval_backfill_limit", 300)
        backfill_min_age_minutes = _var_int("lol_gnn_eval_backfill_min_age_minutes", 10)
        backfill_sleep_sec = _var_float("lol_gnn_eval_backfill_sleep_sec", 0.1)

        now_kst = datetime.now(ZoneInfo("Asia/Seoul")).replace(tzinfo=None)
        rolling_start = now_kst - timedelta(days=window_days)
        cal_host_dir = Variable.get(
            "lol_gnn_pipeline_training_host_dir",
            default="/opt/airflow/dags/git/repo/src/training",
        )
        cal_file = Variable.get("lol_gnn_dataset_calendar_file", default="dataset_calendar.json")
        cal_path = _resolve_calendar_path(cal_host_dir, cal_file)
        with cal_path.open("r", encoding="utf-8") as fp:
            calendar_obj = json.load(fp)
        cal_start = _resolve_current_calendar_start(calendar_obj, now_kst)
        eval_start = max(rolling_start, cal_start)

        hook = PostgresHook(postgres_conn_id=_get_pg_conn_id())
        sql_labeled = """
            SELECT x.pred_blue_win_prob, x.actual_blue_win
            FROM (
                SELECT DISTINCT ON (platform_id, game_id)
                    platform_id,
                    game_id,
                    pred_blue_win_prob,
                    actual_blue_win,
                    created_at
                FROM prediction_request_log
                WHERE status_code = 200
                  AND game_id IS NOT NULL
                  AND actual_blue_win IS NOT NULL
                  AND pred_blue_win_prob IS NOT NULL
                  AND created_at >= %s
                ORDER BY platform_id, game_id, created_at DESC
            ) x;
        """
        rows = hook.get_records(sql_labeled, parameters=(eval_start,))

        if not rows:
            summary = _backfill_missing_actuals(
                hook=hook,
                eval_start=eval_start,
                limit=backfill_limit,
                min_age_minutes=backfill_min_age_minutes,
                sleep_between_sec=backfill_sleep_sec,
            )
            print(f"No labeled rows found; backfill summary: {summary}")
            rows = hook.get_records(sql_labeled, parameters=(eval_start,))

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
            "eval_start": eval_start.strftime("%Y-%m-%d %H:%M:%S"),
        }
        print(
            f"Loaded labeled samples: n={len(preds)} "
            f"window_days={window_days} eval_start={payload['eval_start']}"
        )
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

        # Avoid duplicate runs on days already covered by scheduled build-trainset (+1 / +3).
        now_kst = datetime.now(ZoneInfo("Asia/Seoul")).replace(tzinfo=None)
        cal_host_dir = Variable.get(
            "lol_gnn_pipeline_training_host_dir",
            default="/opt/airflow/dags/git/repo/src/training",
        )
        cal_file = Variable.get("lol_gnn_dataset_calendar_file", default="dataset_calendar.json")
        cal_path = _resolve_calendar_path(cal_host_dir, cal_file)
        try:
            with cal_path.open("r", encoding="utf-8") as fp:
                calendar_obj = json.load(fp)
            if _is_calendar_offset_day(calendar_obj, now_kst):
                print(f"Skip trigger: today is calendar offset run day (+1/+3). now_kst={now_kst}")
                return False
        except Exception as e:
            print(f"Calendar check skipped due to error: {e}")

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
    )

    target_dag_id = Variable.get("lol_gnn_retrain_target_dag_id", default="lol_gnn_build_trainset")
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
