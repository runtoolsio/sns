"""Message formatters for SNS notifications."""

import json

from runtools.runcore.job import InstanceLifecycleEvent
from runtools.runcore.util.dt import format_timedelta_compact


def format_json(event: InstanceLifecycleEvent) -> str:
    """Full serialized event as JSON."""
    return json.dumps(event.serialize(), default=str)


def format_slack(event: InstanceLifecycleEvent) -> str:
    """Slack mrkdwn-formatted summary."""
    run = event.job_run
    meta = run.metadata
    termination = run.lifecycle.termination
    failed = not termination.status.outcome.is_success

    icon = ":x:" if failed else ":white_check_mark:"
    lines = [
        f"{icon}  *[{termination.status.name}] {meta.job_id}@{meta.run_id}*",
        "",
        f"*Job:*  `{meta.job_id}`",
        f"*Run:*  `{meta.run_id}`",
        f"*Status:*  {termination.status.name}",
    ]
    if termination.message:
        lines.append(f"*Message:*  {termination.message}")
    if run.lifecycle.total_run_time:
        lines.append(f"*Duration:*  {format_timedelta_compact(run.lifecycle.total_run_time)}")
    if run.status and run.status.result:
        lines.append(f"*Result:*  {run.status.result.message}")
    if run.status and run.status.warnings:
        warnings = ", ".join(w.message for w in run.status.warnings)
        lines.append(f"*Warnings:*  {warnings}")

    return "\n".join(lines)


FORMATTERS = {
    "json": format_json,
    "slack": format_slack,
}
