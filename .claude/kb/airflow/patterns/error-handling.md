# Error Handling

> **Purpose**: Exponential backoff retries, SLA miss callbacks, failure notification DAGs, Slack and PagerDuty alerting
> **MCP Validated**: 2026-03-26

## When to Use

- Transient failures on external APIs or databases require retry backoff
- SLA breaches must trigger escalation (on-call page, Slack alert)
- Task failures need structured notifications beyond email
- Critical pipelines require a dedicated failure-handling DAG

## Implementation

```python
from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import TaskInstance, DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


# --- 1. Custom retry with exponential backoff ---
def exponential_retry_delay(retry_count: int) -> timedelta:
    """2^n minute backoff: 2m, 4m, 8m, 16m, capped at 30m."""
    return timedelta(minutes=min(2 ** retry_count, 30))


RETRY_ARGS = {
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


# --- 2. on_failure_callback with Slack ---
def slack_failure_callback(context: dict) -> None:
    """Post task failure details to Slack channel."""
    ti: TaskInstance = context["task_instance"]
    dag_id = context["dag"].dag_id
    exec_date = context["execution_date"].isoformat()
    log_url = ti.log_url

    message = (
        f":red_circle: *Task Failed*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Task:* `{ti.task_id}` (attempt {ti.try_number})\n"
        f"*Execution:* `{exec_date}`\n"
        f"*Log:* <{log_url}|View Log>\n"
        f"*Exception:* ```{context.get('exception', 'N/A')}```"
    )

    SlackWebhookOperator(
        task_id="slack_notify",
        slack_webhook_conn_id="slack_data_alerts",
        message=message,
    ).execute(context=context)


# --- 3. SLA miss callback ---
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Escalate SLA breach to on-call via Slack and PagerDuty."""
    missed_tasks = ", ".join(t.task_id for t in task_list)
    SlackWebhookOperator(
        task_id="sla_slack",
        slack_webhook_conn_id="slack_data_alerts",
        message=f":warning: *SLA Miss* on `{dag.dag_id}` -- tasks: {missed_tasks}",
    ).execute(context={})


# --- 4. Trigger failure notification DAG ---
@dag(
    dag_id="failure_notifier",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["alerting", "internal"],
)
def failure_notifier():
    @task
    def format_alert(dag_run: DagRun = None, **context) -> dict:
        conf = context["dag_run"].conf or {}
        return {
            "source_dag": conf.get("source_dag", "unknown"),
            "failed_task": conf.get("failed_task", "unknown"),
            "severity": conf.get("severity", "P3"),
        }

    @task
    def send_pagerduty(alert: dict) -> str:
        """Route P1/P2 to PagerDuty, P3+ to Slack only."""
        if alert["severity"] in ("P1", "P2"):
            from airflow.providers.pagerduty.hooks.pagerduty_events import PagerdutyEventsHook
            hook = PagerdutyEventsHook(pagerduty_events_conn_id="pagerduty_default")
            hook.create_event(
                summary=f"[{alert['severity']}] {alert['source_dag']}.{alert['failed_task']} failed",
                severity="critical" if alert["severity"] == "P1" else "error",
                source="airflow",
            )
            return f"PagerDuty alert sent: {alert['severity']}"
        return f"Skipped PagerDuty for {alert['severity']}"

    @task
    def send_email(alert: dict) -> str:
        from airflow.utils.email import send_email
        send_email(
            to=["data-oncall@company.com"],
            subject=f"[{alert['severity']}] Pipeline failure: {alert['source_dag']}",
            html_content=f"<p>Task <b>{alert['failed_task']}</b> failed in DAG <b>{alert['source_dag']}</b>.</p>",
        )
        return "Email sent"

    alert_data = format_alert()
    send_pagerduty(alert_data)
    send_email(alert_data)


failure_notifier()


# --- 5. Production DAG wired with all error-handling patterns ---
@dag(
    dag_id="production_pipeline",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        **RETRY_ARGS,
        "on_failure_callback": slack_failure_callback,
    },
    sla_miss_callback=sla_miss_callback,
    tags=["production"],
)
def production_pipeline():
    @task(sla=timedelta(hours=1))
    def extract() -> str:
        return "extracted"

    @task(sla=timedelta(hours=2))
    def transform(data: str) -> str:
        return f"transformed({data})"

    notify_on_failure = TriggerDagRunOperator(
        task_id="notify_failure_dag",
        trigger_dag_id="failure_notifier",
        conf={"source_dag": "production_pipeline", "failed_task": "transform", "severity": "P2"},
        trigger_rule="one_failed",
    )

    result = transform(data=extract())
    result >> notify_on_failure


production_pipeline()
```

## Configuration

| Parameter | Location | Purpose |
|-----------|----------|---------|
| `retries` / `retry_delay` | `default_args` | Base retry behaviour |
| `retry_exponential_backoff` | `default_args` | Enable 2^n backoff |
| `max_retry_delay` | `default_args` | Cap backoff ceiling |
| `on_failure_callback` | `default_args` or per-task | Function called on failure |
| `sla_miss_callback` | `@dag()` | Function called on SLA breach |
| `sla` | `@task()` | Max expected duration per task |
| `trigger_rule` | Operator | `one_failed` fires notification task |

## Example Usage

```python
# Minimal retry + Slack alerting on a single task
@task(
    retries=3,
    retry_delay=timedelta(minutes=2),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=15),
    on_failure_callback=slack_failure_callback,
    sla=timedelta(minutes=45),
)
def ingest_api_data() -> str:
    import requests
    resp = requests.get("https://api.vendor.com/data", timeout=30)
    resp.raise_for_status()
    return resp.text
```

## See Also

- [dag-design](../concepts/dag-design.md)
- [task-dependencies](../concepts/task-dependencies.md)
- [sensors-triggers](../patterns/sensors-triggers.md)
- [dag-factory](../patterns/dag-factory.md)
