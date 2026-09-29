#!/usr/bin/env python3

import importlib.util
import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


# mqtt2influx.py imports paho-mqtt and influxdb, but these unit tests
# exercise only payload decoding and queue timestamp handling.
# Stub external modules so the tests do not require production
# dependencies to be installed on the CI/controller host.

paho_module = types.ModuleType("paho")
paho_mqtt_module = types.ModuleType("paho.mqtt")
paho_mqtt_client_module = types.ModuleType("paho.mqtt.client")

paho_module.mqtt = paho_mqtt_module
paho_mqtt_module.client = paho_mqtt_client_module

sys.modules["paho"] = paho_module
sys.modules["paho.mqtt"] = paho_mqtt_module
sys.modules["paho.mqtt.client"] = paho_mqtt_client_module


influxdb_module = types.ModuleType("influxdb")


class DummyInfluxDBClient:
    pass


influxdb_module.InfluxDBClient = DummyInfluxDBClient
sys.modules["influxdb"] = influxdb_module


# mqtt2influx.py reads secret files during module import.
_secret_dir = tempfile.TemporaryDirectory()
_secret_root = Path(_secret_dir.name)

for env_name, filename in (
    ("MQTT_USERNAME_FILE", "mqtt_username"),
    ("MQTT_PASSWORD_FILE", "mqtt_password"),
    ("INFLUX_PASSWORD_FILE", "influx_password"),
):
    secret_path = _secret_root / filename
    secret_path.write_text("test-secret\n", encoding="utf-8")
    os.environ[env_name] = str(secret_path)


source_file = Path(__file__).resolve().parents[1] / "mqtt2influx.py"

spec = importlib.util.spec_from_file_location(
    "mqtt2influx_under_test",
    source_file,
)

mqtt2influx = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mqtt2influx)


class MetricPayloadTests(unittest.TestCase):

    def test_legacy_numeric_payload(self):
        self.assertEqual(
            mqtt2influx.decode_metric_payload("100"),
            ("100", None),
        )

    def test_legacy_string_payload(self):
        self.assertEqual(
            mqtt2influx.decode_metric_payload("online"),
            ("online", None),
        )

    def test_foreign_json_remains_legacy(self):
        raw = (
            '{"schema":"some.other.schema",'
            '"ts":"2026-09-29T17:30:00Z","value":100}'
        )

        self.assertEqual(
            mqtt2influx.decode_metric_payload(raw),
            (raw, None),
        )

    def test_valid_dansu_metric_v1(self):
        raw = json.dumps(
            {
                "schema": "dansu.metric.v1",
                "ts": "2026-09-29T17:30:00.123456Z",
                "value": 100,
            }
        )

        self.assertEqual(
            mqtt2influx.decode_metric_payload(raw),
            (
                "100",
                "2026-09-29T17:30:00.123456Z",
            ),
        )

    def test_invalid_timestamp_falls_back_to_receive_time(self):
        raw = json.dumps(
            {
                "schema": "dansu.metric.v1",
                "ts": "not-a-timestamp",
                "value": 42,
            }
        )

        self.assertEqual(
            mqtt2influx.decode_metric_payload(raw),
            ("42", None),
        )

    def test_missing_timestamp_falls_back_to_receive_time(self):
        raw = json.dumps(
            {
                "schema": "dansu.metric.v1",
                "value": 42,
            }
        )

        self.assertEqual(
            mqtt2influx.decode_metric_payload(raw),
            ("42", None),
        )

    def test_timestamp_without_timezone_falls_back(self):
        raw = json.dumps(
            {
                "schema": "dansu.metric.v1",
                "ts": "2026-09-29T17:30:00.123456",
                "value": 42,
            }
        )

        self.assertEqual(
            mqtt2influx.decode_metric_payload(raw),
            ("42", None),
        )

    def test_timezone_is_normalized_to_utc(self):
        raw = json.dumps(
            {
                "schema": "dansu.metric.v1",
                "ts": "2026-09-29T20:30:00.123456+03:00",
                "value": 12.5,
            }
        )

        self.assertEqual(
            mqtt2influx.decode_metric_payload(raw),
            (
                "12.5",
                "2026-09-29T17:30:00.123456Z",
            ),
        )

    def test_missing_value_preserves_original_payload(self):
        raw = (
            '{"schema":"dansu.metric.v1",'
            '"ts":"2026-09-29T17:30:00Z"}'
        )

        self.assertEqual(
            mqtt2influx.decode_metric_payload(raw),
            (raw, None),
        )

    def test_schedule_item_generates_receive_timestamp(self):
        writer = mqtt2influx.DBWriterThread(object())

        expected = "2026-09-29T17:45:00.000001Z"

        with mock.patch.object(
            mqtt2influx,
            "utc_now_iso",
            return_value=expected,
        ):
            writer.schedule_item(
                "test-client",
                "test-device",
                "test-control",
                "42",
                event_time=None,
            )

        with writer.queue_lock:
            item = writer.data_queue.popleft()

        self.assertEqual(item[0], expected)

        self.assertEqual(
            item[1:],
            (
                "test-client",
                "test-device",
                "test-control",
                "42",
            ),
        )


    def test_source_timestamp_reaches_influx_point(self):
        writer = mqtt2influx.DBWriterThread(object())

        source_time = "2026-09-29T17:30:00.123456Z"

        writer.schedule_item(
            "wb_AMTXYWW3",
            "wan_probe_primary",
            "Packet_loss_pct",
            "100",
            event_time=source_time,
        )

        with writer.queue_lock:
            item = writer.data_queue.popleft()

        point = writer.serialize_data_item(*item)

        self.assertEqual(
            point["time"],
            source_time,
        )
        self.assertEqual(
            point["tags"],
            {
                "client": "wb_AMTXYWW3",
                "channel": "wan_probe_primary/Packet_loss_pct",
            },
        )
        self.assertEqual(
            point["fields"],
            {"value_f": 100.0},
        )


if __name__ == "__main__":
    unittest.main()
