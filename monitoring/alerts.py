"""
Enterprise Observability & Failure Alerting Module
Provides:
1. Airflow on_failure_callback for Slack Webhooks
2. Airflow on_failure_callback for SMTP Email Alerts
3. Structured JSON Logging and Latency Tracker
"""

import os
import sys
import json
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
import requests

logger = logging.getLogger("LakehouseAlerts")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
ALERT_EMAIL_RECIPIENT = os.getenv("ALERT_EMAIL_RECIPIENT", "admin@lakehouse.local")
ALERT_EMAIL_SENDER = os.getenv("ALERT_EMAIL_SENDER", "airflow-alerts@lakehouse.local")
SMTP_SERVER = os.getenv("SMTP_SERVER", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", "25"))


def send_slack_notification(context, webhook_url=SLACK_WEBHOOK_URL):
    """
    Sends a formatted Slack incident card when an Airflow task or pipeline fails.
    """
    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL is not set. Simulating Slack alert to logs.")
        print(f"[SIMULATED SLACK ALERT] Task failed: {context.get('task_instance_key_str')}")
        return

    ti = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = ti.task_id if ti else "unknown"
    exec_date = str(context.get("execution_date", datetime.now(timezone.utc)))
    log_url = ti.log_url if ti else "N/A"
    exception = str(context.get("exception", "Unknown Exception"))

    slack_message = {
        "text": f"🚨 *Airflow Pipeline Failure Alert: {dag_id}*",
        "attachments": [
            {
                "color": "#D32F2F",
                "fields": [
                    {"title": "DAG", "value": dag_id, "short": True},
                    {"title": "Task", "value": task_id, "short": True},
                    {"title": "Execution Time", "value": exec_date, "short": False},
                    {"title": "Exception", "value": f"```{exception[:500]}```", "short": False},
                    {"title": "Log URL", "value": f"<{log_url}|View Task Logs>", "short": False}
                ],
                "footer": "Lakehouse Observability Engine",
                "ts": int(datetime.now(timezone.utc).timestamp())
            }
        ]
    }

    try:
        response = requests.post(
            webhook_url,
            data=json.dumps(slack_message),
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        if response.status_code == 200:
            logger.info("Slack alert dispatched successfully.")
        else:
            logger.error(f"Slack alert failed with status code {response.status_code}: {response.text}")
    except Exception as e:
        logger.error(f"Failed to post Slack alert: {e}")


def send_email_notification(context, recipient=ALERT_EMAIL_RECIPIENT):
    """
    Sends an SMTP email notification upon task failure.
    """
    ti = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = ti.task_id if ti else "unknown"
    exception = str(context.get("exception", "No error trace available"))

    subject = f"[CRITICAL ALERT] Airflow Failure: {dag_id}.{task_id}"
    body = f"""
    <h2>Airflow Task Failure Alert</h2>
    <p><b>DAG ID:</b> {dag_id}</p>
    <p><b>Task ID:</b> {task_id}</p>
    <p><b>Execution Date:</b> {context.get('execution_date')}</p>
    <p><b>Exception Details:</b></p>
    <pre>{exception}</pre>
    <p>Please check the Lakehouse monitoring console.</p>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = ALERT_EMAIL_SENDER
    msg["To"] = recipient
    msg.attach(MIMEText(body, "html"))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=5) as server:
            server.sendmail(ALERT_EMAIL_SENDER, [recipient], msg.as_string())
        logger.info(f"Email failure alert dispatched to {recipient}.")
    except Exception as e:
        logger.warning(f"Could not dispatch email via SMTP ({e}). Fallback to standard logging.")


def airflow_failure_callback(context):
    """
    Unified failure handler for Airflow tasks.
    Can be assigned directly: default_args={'on_failure_callback': airflow_failure_callback}
    """
    logger.error("Triggering pipeline failure callbacks...")
    send_slack_notification(context)
    send_email_notification(context)

