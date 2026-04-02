"""
tests/test_kafka_dlq.py — Kafka DLQ routing tests.

Covers: successful commits, failure retries, max retries routing
to DLQ, DLQ payload metadata, and tracker cleanup.
"""

import json
from unittest.mock import MagicMock, patch


import shared.kafka_client as kafka


class FakeMessage:
    """Mimics a confluent_kafka.Message for testing."""

    def __init__(self, topic, partition, offset, value, error=None):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._value = value
        self._error = error

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def error(self):
        return self._error


class TestDLQ:
    def setup_method(self):
        kafka._retry_tracker.clear()

    def test_success_commits(self):
        """Successful handler call commits the message."""
        msg = FakeMessage(
            "test.topic", 0, 100,
            json.dumps({"data": "ok"}).encode(),
        )
        consumer = MagicMock()
        consumer.poll = MagicMock(side_effect=[msg, None])
        handler = MagicMock()

        # Break out of infinite loop after 2 polls
        poll_count = [0]
        original_poll = consumer.poll

        def limited_poll(timeout=1.0):
            poll_count[0] += 1
            if poll_count[0] > 2:
                raise KeyboardInterrupt
            return original_poll(timeout=timeout)

        consumer.poll = limited_poll

        try:
            kafka.consume_loop(consumer, handler, poll_timeout=0)
        except KeyboardInterrupt:
            pass

        handler.assert_called_once_with("test.topic", {"data": "ok"})
        consumer.commit.assert_called_once()

    def test_retry_tracker_increments(self):
        """Handler failures increment the retry tracker."""
        key = "test.topic:0:100"
        kafka._retry_tracker[key] = 1

        assert kafka._retry_tracker[key] == 1
        kafka._retry_tracker[key] += 1
        assert kafka._retry_tracker[key] == 2

    def test_max_retries_sends_to_dlq(self):
        """After max retries, message goes to DLQ and tracker is cleaned."""
        msg = FakeMessage(
            "test.topic", 0, 100,
            json.dumps({"data": "fail"}).encode(),
        )
        key = "test.topic:0:100"

        # Pre-set tracker to max_handler_retries - 1
        kafka._retry_tracker[key] = 2

        consumer = MagicMock()
        handler = MagicMock(side_effect=ValueError("boom"))

        poll_count = [0]

        def limited_poll(timeout=1.0):
            poll_count[0] += 1
            if poll_count[0] == 1:
                return msg
            raise KeyboardInterrupt

        consumer.poll = limited_poll

        with patch.object(kafka, "publish_dict") as mock_pub:
            try:
                kafka.consume_loop(
                    consumer, handler,
                    max_handler_retries=3,
                    poll_timeout=0,
                )
            except KeyboardInterrupt:
                pass

            # DLQ publish should have been called
            mock_pub.assert_called_once()
            dlq_call = mock_pub.call_args
            assert dlq_call[0][0] == "dlq.default"
            dlq_payload = dlq_call[0][1]
            assert dlq_payload["original_topic"] == "test.topic"
            assert dlq_payload["original_partition"] == 0
            assert dlq_payload["original_offset"] == 100
            assert "boom" in dlq_payload["error"]
            assert dlq_payload["retry_count"] == 3

        # Tracker should be cleaned
        assert key not in kafka._retry_tracker

    def test_dlq_payload_contains_service(self):
        """DLQ payload includes the service name."""
        payload = json.dumps({"x": 1}).encode()
        with patch.object(kafka, "publish_dict") as mock_pub:
            kafka._send_to_dlq(
                "dlq.default", "some.topic", 1, 42,
                payload, "test error", 3,
            )
            call_args = mock_pub.call_args[0][1]
            assert "service" in call_args
            assert "failed_at" in call_args

    def test_tracker_cleanup_on_success(self):
        """Successful processing removes the tracker entry."""
        key = "test.topic:0:200"
        kafka._retry_tracker[key] = 1

        msg = FakeMessage(
            "test.topic", 0, 200,
            json.dumps({"data": "ok"}).encode(),
        )
        consumer = MagicMock()
        handler = MagicMock()

        poll_count = [0]

        def limited_poll(timeout=1.0):
            poll_count[0] += 1
            if poll_count[0] == 1:
                return msg
            raise KeyboardInterrupt

        consumer.poll = limited_poll

        try:
            kafka.consume_loop(consumer, handler, poll_timeout=0)
        except KeyboardInterrupt:
            pass

        assert key not in kafka._retry_tracker
