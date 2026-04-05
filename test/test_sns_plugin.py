import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from runtools.runcore.job import JobRun, JobInstanceMetadata, InstanceID, InstanceLifecycleEvent
from runtools.runcore.plugins import PluginDisabledError
from runtools.runcore.run import TerminationStatus, TerminationInfo, RunLifecycle, PhaseRun, Stage
from runtools.sns.plugin import SnsPlugin, _parse_rules


# --- Rule parsing ---

def test_parse_valid_rule():
    rules = _parse_rules([{"term_status": "failed", "topic_arn": "arn:topic"}])
    assert len(rules) == 1
    assert rules[0].term_status == TerminationStatus.FAILED


def test_parse_case_insensitive():
    rules = _parse_rules([{"term_status": "Failed", "topic_arn": "arn:topic"}])
    assert rules[0].term_status == TerminationStatus.FAILED


def test_parse_invalid_status_skipped():
    rules = _parse_rules([{"term_status": "bogus", "topic_arn": "arn:topic"}])
    assert len(rules) == 0


def test_parse_missing_topic_skipped():
    rules = _parse_rules([{"term_status": "failed"}])
    assert len(rules) == 0


def test_parse_empty_rules():
    rules = _parse_rules([])
    assert len(rules) == 0


def test_parse_format_defaults_to_json():
    rules = _parse_rules([{"term_status": "failed", "topic_arn": "arn:topic"}])
    from runtools.sns.formatters import format_json
    assert rules[0].formatter is format_json


def test_parse_format_slack():
    rules = _parse_rules([{"term_status": "failed", "topic_arn": "arn:topic", "format": "slack"}])
    from runtools.sns.formatters import format_slack
    assert rules[0].formatter is format_slack


def test_parse_invalid_format_skipped():
    rules = _parse_rules([{"term_status": "failed", "topic_arn": "arn:topic", "format": "xml"}])
    assert len(rules) == 0


# --- Plugin init ---

@patch("runtools.sns.plugin.boto3")
def test_plugin_disabled_on_empty_rules(mock_boto3):
    with pytest.raises(PluginDisabledError):
        SnsPlugin({})


@patch("runtools.sns.plugin.boto3")
def test_plugin_disabled_on_invalid_rules(mock_boto3):
    with pytest.raises(PluginDisabledError):
        SnsPlugin({"rules": [{"term_status": "bogus", "topic_arn": "arn:topic"}]})


@patch("runtools.sns.plugin.boto3")
def test_plugin_initializes_with_valid_rules(mock_boto3):
    plugin = SnsPlugin({"rules": [{"term_status": "failed", "topic_arn": "arn:topic"}]})
    assert len(plugin._rules) == 1


# --- Notification ---

def _make_event(status: TerminationStatus, job_id="test-job", run_id="run1", message=None):
    now = datetime(2026, 1, 1)
    termination = TerminationInfo(status, now, message=message)
    lifecycle = RunLifecycle(created_at=now, started_at=now, termination=termination)
    root_phase = PhaseRun(
        phase_id="root", phase_type="FUNCTION", is_idle=False,
        attributes=None, variables=None, lifecycle=lifecycle, children=(),
    )
    metadata = JobInstanceMetadata(InstanceID(job_id, run_id))
    run = JobRun(metadata, root_phase=root_phase)
    return InstanceLifecycleEvent(run, Stage.ENDED, now)


@patch("runtools.sns.plugin.boto3")
def test_matching_rule_publishes_json(mock_boto3):
    mock_sns = MagicMock()
    mock_boto3.client.return_value = mock_sns

    plugin = SnsPlugin({"rules": [{"term_status": "failed", "topic_arn": "arn:topic"}]})
    event = _make_event(TerminationStatus.FAILED, message="Exit code: 1")

    plugin.instance_lifecycle_update(event)

    mock_sns.publish.assert_called_once()
    call_kwargs = mock_sns.publish.call_args[1]
    assert call_kwargs["TopicArn"] == "arn:topic"
    assert "FAILED" in call_kwargs["Subject"]
    message = json.loads(call_kwargs["Message"])
    assert message["event"]["new_stage"] == "ENDED"
    assert message["event_metadata"]["instance"]["job_id"] == "test-job"


@patch("runtools.sns.plugin.boto3")
def test_matching_rule_publishes_slack(mock_boto3):
    mock_sns = MagicMock()
    mock_boto3.client.return_value = mock_sns

    plugin = SnsPlugin({"rules": [{"term_status": "failed", "topic_arn": "arn:topic", "format": "slack"}]})
    event = _make_event(TerminationStatus.FAILED, message="Exit code: 1")

    plugin.instance_lifecycle_update(event)

    mock_sns.publish.assert_called_once()
    call_kwargs = mock_sns.publish.call_args[1]
    msg = call_kwargs["Message"]
    assert ":x:" in msg
    assert "*Job:*" in msg
    assert "`test-job`" in msg
    assert "Exit code: 1" in msg


@patch("runtools.sns.plugin.boto3")
def test_non_matching_rule_does_not_publish(mock_boto3):
    mock_sns = MagicMock()
    mock_boto3.client.return_value = mock_sns

    plugin = SnsPlugin({"rules": [{"term_status": "failed", "topic_arn": "arn:topic"}]})
    event = _make_event(TerminationStatus.COMPLETED)

    plugin.instance_lifecycle_update(event)

    mock_sns.publish.assert_not_called()


@patch("runtools.sns.plugin.boto3")
def test_publish_failure_does_not_raise(mock_boto3):
    mock_sns = MagicMock()
    mock_sns.publish.side_effect = Exception("SNS unavailable")
    mock_boto3.client.return_value = mock_sns

    plugin = SnsPlugin({"rules": [{"term_status": "failed", "topic_arn": "arn:topic"}]})
    event = _make_event(TerminationStatus.FAILED)

    plugin.instance_lifecycle_update(event)  # should not raise
