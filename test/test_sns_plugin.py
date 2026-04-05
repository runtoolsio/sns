import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from runtools.runcore.job import JobRun, JobInstanceMetadata, InstanceID, InstanceLifecycleEvent
from runtools.runcore.plugins import PluginDisabledError
from runtools.runcore.run import TerminationStatus, TerminationInfo, RunLifecycle, PhaseRun, Stage
from runtools.sns.plugin import SnsPlugin, _parse_rules


# --- Rule parsing ---

def test_parse_defaults_to_ended_stage():
    rules = _parse_rules([{"topic_arn": "arn:topic"}])
    assert len(rules) == 1
    assert rules[0].stages == {Stage.ENDED}
    assert rules[0].term_statuses == set()


def test_parse_stage_string():
    rules = _parse_rules([{"stage": "running", "topic_arn": "arn:topic"}])
    assert rules[0].stages == {Stage.RUNNING}


def test_parse_stage_array():
    rules = _parse_rules([{"stage": ["running", "ended"], "topic_arn": "arn:topic"}])
    assert rules[0].stages == {Stage.RUNNING, Stage.ENDED}


def test_parse_term_status_array():
    rules = _parse_rules([{"term_status": ["failed", "stopped"], "topic_arn": "arn:topic"}])
    assert rules[0].term_statuses == {TerminationStatus.FAILED, TerminationStatus.STOPPED}


def test_parse_term_status_string():
    rules = _parse_rules([{"term_status": "failed", "topic_arn": "arn:topic"}])
    assert rules[0].term_statuses == {TerminationStatus.FAILED}


def test_parse_invalid_term_status_skips_rule():
    rules = _parse_rules([{"term_status": "bogus", "topic_arn": "arn:topic"}])
    assert len(rules) == 0


def test_parse_invalid_stage_skips_rule():
    rules = _parse_rules([{"stage": "bogus", "topic_arn": "arn:topic"}])
    assert len(rules) == 0


def test_parse_outcome_expands_to_term_statuses():
    rules = _parse_rules([{"outcome": "fault", "topic_arn": "arn:topic"}])
    assert len(rules) == 1
    assert TerminationStatus.FAILED in rules[0].term_statuses
    assert TerminationStatus.ERROR in rules[0].term_statuses
    assert TerminationStatus.COMPLETED not in rules[0].term_statuses


def test_parse_outcome_array():
    rules = _parse_rules([{"outcome": ["fault", "aborted"], "topic_arn": "arn:topic"}])
    assert TerminationStatus.FAILED in rules[0].term_statuses
    assert TerminationStatus.STOPPED in rules[0].term_statuses


def test_parse_outcome_combined_with_term_status():
    rules = _parse_rules([{"outcome": "aborted", "term_status": "failed", "topic_arn": "arn:topic"}])
    assert TerminationStatus.FAILED in rules[0].term_statuses
    assert TerminationStatus.STOPPED in rules[0].term_statuses


def test_parse_invalid_outcome_skips_rule():
    rules = _parse_rules([{"outcome": "bogus", "topic_arn": "arn:topic"}])
    assert len(rules) == 0


def test_parse_case_insensitive():
    rules = _parse_rules([{"stage": "Running", "term_status": "Failed", "topic_arn": "arn:topic"}])
    assert rules[0].stages == {Stage.RUNNING}
    assert rules[0].term_statuses == {TerminationStatus.FAILED}


def test_parse_missing_topic_skipped():
    rules = _parse_rules([{"stage": "ended"}])
    assert len(rules) == 0


def test_parse_invalid_format_skipped():
    rules = _parse_rules([{"topic_arn": "arn:topic", "format": "xml"}])
    assert len(rules) == 0


def test_parse_format_defaults_to_json():
    rules = _parse_rules([{"topic_arn": "arn:topic"}])
    from runtools.sns.formatters import format_json
    assert rules[0].formatter is format_json


def test_parse_format_slack():
    rules = _parse_rules([{"topic_arn": "arn:topic", "format": "slack"}])
    from runtools.sns.formatters import format_slack
    assert rules[0].formatter is format_slack


# --- Plugin init ---

@patch("runtools.sns.plugin.boto3")
def test_plugin_disabled_on_empty_rules(mock_boto3):
    with pytest.raises(PluginDisabledError):
        SnsPlugin({})


@patch("runtools.sns.plugin.boto3")
def test_plugin_initializes_with_valid_rules(mock_boto3):
    plugin = SnsPlugin({"rules": [{"topic_arn": "arn:topic"}]})
    assert len(plugin._rules) == 1


# --- Rule matching ---

def _make_event(stage: Stage, status: TerminationStatus = None, job_id="test-job", run_id="run1", message=None):
    now = datetime(2026, 1, 1)
    if status:
        termination = TerminationInfo(status, now, message=message)
    else:
        termination = None
    started = now if stage in (Stage.RUNNING, Stage.ENDED) else None
    lifecycle = RunLifecycle(created_at=now, started_at=started, termination=termination)
    root_phase = PhaseRun(
        phase_id="root", phase_type="FUNCTION", is_idle=False,
        attributes=None, variables=None, lifecycle=lifecycle, children=(),
    )
    metadata = JobInstanceMetadata(InstanceID(job_id, run_id))
    run = JobRun(metadata, root_phase=root_phase)
    return InstanceLifecycleEvent(run, stage, now)


@patch("runtools.sns.plugin.boto3")
def test_ended_failed_matches(mock_boto3):
    mock_sns = MagicMock()
    mock_boto3.client.return_value = mock_sns

    plugin = SnsPlugin({"rules": [{"term_status": "failed", "topic_arn": "arn:topic"}]})
    plugin.instance_lifecycle_update(_make_event(Stage.ENDED, TerminationStatus.FAILED))

    mock_sns.publish.assert_called_once()


@patch("runtools.sns.plugin.boto3")
def test_ended_completed_does_not_match_failed_rule(mock_boto3):
    mock_sns = MagicMock()
    mock_boto3.client.return_value = mock_sns

    plugin = SnsPlugin({"rules": [{"term_status": "failed", "topic_arn": "arn:topic"}]})
    plugin.instance_lifecycle_update(_make_event(Stage.ENDED, TerminationStatus.COMPLETED))

    mock_sns.publish.assert_not_called()


@patch("runtools.sns.plugin.boto3")
def test_running_stage_matches(mock_boto3):
    mock_sns = MagicMock()
    mock_boto3.client.return_value = mock_sns

    plugin = SnsPlugin({"rules": [{"stage": "running", "topic_arn": "arn:topic"}]})
    plugin.instance_lifecycle_update(_make_event(Stage.RUNNING))

    mock_sns.publish.assert_called_once()


@patch("runtools.sns.plugin.boto3")
def test_running_does_not_match_ended_rule(mock_boto3):
    mock_sns = MagicMock()
    mock_boto3.client.return_value = mock_sns

    plugin = SnsPlugin({"rules": [{"stage": "ended", "topic_arn": "arn:topic"}]})
    plugin.instance_lifecycle_update(_make_event(Stage.RUNNING))

    mock_sns.publish.assert_not_called()


@patch("runtools.sns.plugin.boto3")
def test_multi_stage_rule(mock_boto3):
    mock_sns = MagicMock()
    mock_boto3.client.return_value = mock_sns

    plugin = SnsPlugin({"rules": [{"stage": ["running", "ended"], "topic_arn": "arn:topic"}]})
    plugin.instance_lifecycle_update(_make_event(Stage.RUNNING))
    plugin.instance_lifecycle_update(_make_event(Stage.ENDED, TerminationStatus.COMPLETED))

    assert mock_sns.publish.call_count == 2


@patch("runtools.sns.plugin.boto3")
def test_slack_format_in_message(mock_boto3):
    mock_sns = MagicMock()
    mock_boto3.client.return_value = mock_sns

    plugin = SnsPlugin({"rules": [{"term_status": "failed", "topic_arn": "arn:topic", "format": "slack"}]})
    plugin.instance_lifecycle_update(_make_event(Stage.ENDED, TerminationStatus.FAILED, message="Exit code: 1"))

    msg = mock_sns.publish.call_args[1]["Message"]
    assert ":x:" in msg
    assert "*Job:*" in msg
    assert "`test-job`" in msg


@patch("runtools.sns.plugin.boto3")
def test_publish_failure_does_not_raise(mock_boto3):
    mock_sns = MagicMock()
    mock_sns.publish.side_effect = Exception("SNS unavailable")
    mock_boto3.client.return_value = mock_sns

    plugin = SnsPlugin({"rules": [{"term_status": "failed", "topic_arn": "arn:topic"}]})
    plugin.instance_lifecycle_update(_make_event(Stage.ENDED, TerminationStatus.FAILED))
