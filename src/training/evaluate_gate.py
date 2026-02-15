import argparse
import json
import os
import sys
from typing import Dict, Tuple

from mlflow.tracking import MlflowClient


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_run_id(
    client: MlflowClient,
    experiment_name: str,
    run_id_file: str = "",
    pipeline_run_id: str = "",
) -> str:
    if run_id_file and os.path.exists(run_id_file):
        run_id = open(run_id_file, "r", encoding="utf-8").read().strip()
        if run_id:
            return run_id

    exp = client.get_experiment_by_name(experiment_name)
    if exp is None:
        raise ValueError(f"Experiment not found: {experiment_name}")

    filter_string = ""
    if pipeline_run_id:
        safe_run_id = pipeline_run_id.replace("'", "\\'")
        filter_string = f"tags.pipeline_run_id = '{safe_run_id}'"

    runs = client.search_runs(
        experiment_ids=[exp.experiment_id],
        filter_string=filter_string,
        order_by=["attributes.start_time DESC"],
        max_results=1,
    )
    if not runs:
        if pipeline_run_id:
            raise ValueError(
                f"No runs found for experiment={experiment_name} with pipeline_run_id={pipeline_run_id}"
            )
        raise ValueError(f"No runs found for experiment: {experiment_name}")

    return runs[0].info.run_id


def check_thresholds(metrics: Dict[str, float], criteria: dict) -> Tuple[bool, list]:
    failures = []
    for metric_name, rule in criteria.get("metrics", {}).items():
        if metric_name not in metrics:
            failures.append(f"Missing metric: {metric_name}")
            continue

        value = float(metrics[metric_name])

        if "min" in rule and value < float(rule["min"]):
            failures.append(f"{metric_name}={value:.6f} < min({rule['min']})")

        if "max" in rule and value > float(rule["max"]):
            failures.append(f"{metric_name}={value:.6f} > max({rule['max']})")

    return len(failures) == 0, failures


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate MLflow run against quality gate criteria")
    parser.add_argument("--config", default="config.json", help="Path to train config.json")
    parser.add_argument("--criteria", default="evaluation_criteria.json", help="Path to gate criteria JSON")
    parser.add_argument("--run-id-file", default="", help="File containing MLflow run_id")
    parser.add_argument("--pipeline-run-id", default="", help="Orchestration run id for exact run lookup")
    parser.add_argument("--promote-on-pass", action="store_true", help="Set production tag on gate pass")
    parser.add_argument("--promote-tag-key", default="status", help="Tag key for promotion")
    parser.add_argument("--promote-tag-value", default="production", help="Tag value for promotion")
    parser.add_argument("--demote-tag-value", default="regacy", help="Tag value for previous production runs")
    args = parser.parse_args()

    config = load_json(args.config)
    criteria = load_json(args.criteria)

    tracking_uri = config["mlflow"]["uri"]
    experiment_name = config["mlflow"]["experiment_name"]

    client = MlflowClient(tracking_uri=tracking_uri)
    run_id = get_run_id(
        client=client,
        experiment_name=experiment_name,
        run_id_file=args.run_id_file,
        pipeline_run_id=args.pipeline_run_id,
    )
    run = client.get_run(run_id)

    metrics = run.data.metrics
    passed, failures = check_thresholds(metrics, criteria)

    print(f"Run ID: {run_id}")
    print("Collected metrics:")
    for key in sorted(metrics.keys()):
        print(f"- {key}: {metrics[key]}")

    if not passed:
        print("\n[FAIL] Model quality gate failed:")
        for msg in failures:
            print(f"- {msg}")
        sys.exit(1)

    if args.promote_on_pass:
        exp = client.get_experiment_by_name(experiment_name)
        if exp is None:
            raise ValueError(f"Experiment not found: {experiment_name}")

        existing_prod_runs = client.search_runs(
            experiment_ids=[exp.experiment_id],
            filter_string=f"tags.{args.promote_tag_key} = '{args.promote_tag_value}'",
            order_by=["attributes.start_time DESC"],
            max_results=5000,
        )
        for prod_run in existing_prod_runs:
            if prod_run.info.run_id == run_id:
                continue
            client.set_tag(prod_run.info.run_id, args.promote_tag_key, args.demote_tag_value)

        client.set_tag(run_id, args.promote_tag_key, args.promote_tag_value)
        print(
            f"\n[PASS] Model quality gate passed. "
            f"Set tag {args.promote_tag_key}={args.promote_tag_value} on run {run_id} "
            f"and demoted previous production runs to {args.demote_tag_value}"
        )
    else:
        print("\n[PASS] Model quality gate passed")


if __name__ == "__main__":
    main()
