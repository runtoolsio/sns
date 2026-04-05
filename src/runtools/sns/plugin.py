"""SNS notification plugin — sends notifications to AWS SNS topics based on job lifecycle events."""

import logging
from typing import Callable, Dict, List, Set

import boto3

from runtools.runcore.job import JobInstance, InstanceLifecycleEvent, InstanceLifecycleObserver
from runtools.runcore.plugins import Plugin, PluginDisabledError
from runtools.runcore.run import Outcome, Stage, TerminationStatus
from runtools.sns.formatters import FORMATTERS

log = logging.getLogger(__name__)


class SnsPlugin(Plugin, InstanceLifecycleObserver, plugin_name='sns'):
    """Plugin that publishes SNS notifications on job lifecycle events.

    Config example::

        [[plugins.sns.rules]]
        stage = ["ended"]
        term_status = ["failed"]
        topic_arn = "arn:aws:sns:eu-west-1:123:alerts-critical"
        format = "slack"
    """

    def __init__(self, config: dict):
        self._rules = _parse_rules(config.get('rules', []))
        if not self._rules:
            raise PluginDisabledError("No valid SNS rules configured")
        self._sns_client = boto3.client('sns')
        log.info("event=[sns_plugin_initialized] rules=[%d]", len(self._rules))

    def on_instance_added(self, job_instance: JobInstance):
        job_instance.notifications.add_observer_lifecycle(self)

    def on_instance_removed(self, job_instance: JobInstance):
        job_instance.notifications.remove_observer_lifecycle(self)

    def instance_lifecycle_update(self, event: InstanceLifecycleEvent):
        for rule in self._rules:
            if rule.matches(event):
                try:
                    _publish(self._sns_client, rule.topic_arn, rule.formatter, event)
                except Exception:
                    log.exception("event=[sns_publish_failed] topic=[%s] instance=[%s]",
                                  rule.topic_arn, event.job_run.metadata.instance_id)


class _Rule:
    """Notification rule: stage + optional term_status filter -> topic_arn + format."""

    def __init__(self, stages: Set[Stage], term_statuses: Set[TerminationStatus],
                 topic_arn: str, formatter: Callable):
        self.stages = stages
        self.term_statuses = term_statuses
        self.topic_arn = topic_arn
        self.formatter = formatter

    def matches(self, event: InstanceLifecycleEvent) -> bool:
        if event.new_stage not in self.stages:
            return False
        if self.term_statuses:
            termination = event.job_run.lifecycle.termination
            if not termination or termination.status not in self.term_statuses:
                return False
        return True


def _publish(sns_client, topic_arn: str, formatter: Callable, event: InstanceLifecycleEvent):
    meta = event.job_run.metadata
    status_name = event.new_stage.name
    termination = event.job_run.lifecycle.termination
    if termination:
        status_name = termination.status.name
    subject = f"[{status_name}] {meta.job_id}@{meta.run_id}"

    sns_client.publish(
        TopicArn=topic_arn,
        Subject=subject[:100],
        Message=formatter(event),
    )
    log.info("event=[sns_notification_sent] topic=[%s] instance=[%s] stage=[%s]",
             topic_arn, meta.instance_id, event.new_stage.name)


def _parse_rules(rules_config: List[Dict]) -> List[_Rule]:
    """Parse rule dicts into _Rule objects."""
    rules = []
    for entry in rules_config:
        topic_arn = entry.get('topic_arn')
        if not topic_arn:
            log.warning("event=[sns_rule_missing_topic] rule=[%s]", entry)
            continue

        # Parse stages
        stages = _parse_enum_set(entry, 'stage', Stage, default={Stage.ENDED})
        if stages is None:
            continue

        # Parse term_statuses — from explicit term_status and/or outcome
        term_statuses = _parse_enum_set(entry, 'term_status', TerminationStatus, default=set())
        if term_statuses is None:
            continue
        outcomes = _parse_enum_set(entry, 'outcome', Outcome, default=set())
        if outcomes is None:
            continue
        if outcomes:
            term_statuses = term_statuses | TerminationStatus.get_statuses(*outcomes)

        # Parse format
        format_name = entry.get('format', 'json')
        formatter = FORMATTERS.get(format_name)
        if not formatter:
            log.warning("event=[sns_rule_invalid_format] format=[%s] valid=[%s]",
                        format_name, list(FORMATTERS))
            continue

        rules.append(_Rule(stages, term_statuses, topic_arn, formatter))
    return rules


def _parse_enum_set(entry: dict, key: str, enum_cls, default=None) -> set | None:
    """Parse a string or list of strings into a set of enum values.

    Returns:
        Set of enum values, default if key is absent, or None if key is present but all values are invalid.
    """
    raw = entry.get(key)
    if raw is None:
        return default if default is not None else set()
    if isinstance(raw, str):
        raw = [raw]
    result = set()
    for name in raw:
        try:
            result.add(enum_cls[name.upper()])
        except KeyError:
            log.warning("event=[sns_rule_invalid_%s] value=[%s] valid=[%s]",
                        key, name, [e.name for e in enum_cls])
    if not result:
        log.warning("event=[sns_rule_skipped] reason=[no valid %s values] rule=[%s]", key, entry)
        return None
    return result
