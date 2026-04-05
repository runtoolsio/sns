"""SNS notification plugin — sends notifications to AWS SNS topics based on job termination status."""

import logging
from typing import Callable, Dict, List

import boto3

from runtools.runcore.job import JobInstance, InstanceLifecycleEvent, InstanceLifecycleObserver
from runtools.runcore.plugins import Plugin, PluginDisabledError
from runtools.runcore.run import TerminationStatus
from runtools.sns.formatters import FORMATTERS

log = logging.getLogger(__name__)


class SnsPlugin(Plugin, InstanceLifecycleObserver, plugin_name='sns'):
    """Plugin that publishes SNS notifications when jobs reach configured terminal states.

    Config example::

        [[plugins.sns.rules]]
        term_status = "failed"
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
        termination = event.job_run.lifecycle.termination
        if not termination:
            return

        for rule in self._rules:
            if rule.matches(termination.status):
                try:
                    _publish(self._sns_client, rule.topic_arn, rule.formatter, event)
                except Exception:
                    log.exception("event=[sns_publish_failed] topic=[%s] instance=[%s]",
                                  rule.topic_arn, event.job_run.metadata.instance_id)


class _Rule:
    """A single notification rule: term_status -> topic_arn + format."""

    def __init__(self, term_status: TerminationStatus, topic_arn: str, formatter: Callable):
        self.term_status = term_status
        self.topic_arn = topic_arn
        self.formatter = formatter

    def matches(self, status: TerminationStatus) -> bool:
        return self.term_status == status


def _publish(sns_client, topic_arn: str, formatter: Callable, event: InstanceLifecycleEvent):
    meta = event.job_run.metadata
    termination = event.job_run.lifecycle.termination
    subject = f"[{termination.status.name}] {meta.job_id}@{meta.run_id}"

    sns_client.publish(
        TopicArn=topic_arn,
        Subject=subject[:100],
        Message=formatter(event),
    )
    log.info("event=[sns_notification_sent] topic=[%s] instance=[%s] status=[%s]",
             topic_arn, meta.instance_id, termination.status.name)


def _parse_rules(rules_config: List[Dict]) -> List[_Rule]:
    """Parse rule dicts into _Rule objects."""
    rules = []
    for entry in rules_config:
        status_name = entry.get('term_status', '').upper()
        topic_arn = entry.get('topic_arn')
        if not topic_arn:
            log.warning("event=[sns_rule_missing_topic] rule=[%s]", entry)
            continue
        try:
            status = TerminationStatus[status_name]
        except KeyError:
            log.warning("event=[sns_rule_invalid_status] status=[%s] valid=[%s]",
                        status_name, [s.name for s in TerminationStatus])
            continue
        format_name = entry.get('format', 'json')
        formatter = FORMATTERS.get(format_name)
        if not formatter:
            log.warning("event=[sns_rule_invalid_format] format=[%s] valid=[%s]",
                        format_name, list(FORMATTERS))
            continue
        rules.append(_Rule(status, topic_arn, formatter))
    return rules
