#!/usr/bin/python3

import logging
import math
import os
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock, Thread

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient


logging.basicConfig(
    format="%(asctime)s %(levelname)s: %(message)s",
    level=logging.INFO,
)


def env_int(name, default):
    value = os.getenv(name, str(default))
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(
            f"Environment variable {name} must be an integer"
        ) from exc


def env_float(name, default):
    value = os.getenv(name, str(default))
    try:
        return float(value)
    except ValueError as exc:
        raise RuntimeError(
            f"Environment variable {name} must be a number"
        ) from exc


def read_secret(env_name, default_path):
    path = Path(os.getenv(env_name, default_path))

    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise RuntimeError(
            f"Unable to read secret file configured by {env_name}: {path}"
        ) from exc

    if not value:
        raise RuntimeError(
            f"Secret file configured by {env_name} is empty: {path}"
        )

    return value


MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = env_int("MQTT_PORT", 1883)
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "#")

INFLUX_HOST = os.getenv("INFLUX_HOST", "influxdb")
INFLUX_PORT = env_int("INFLUX_PORT", 8086)
INFLUX_DATABASE = os.getenv("INFLUX_DATABASE", "mqtt_data")
INFLUX_USERNAME = os.getenv("INFLUX_USERNAME", "mqtt2influx_wr")
INFLUX_TIMEOUT = env_float("INFLUX_TIMEOUT", 5)
INFLUX_RETRY_INITIAL = env_float("INFLUX_RETRY_INITIAL", 1)
INFLUX_RETRY_MAX = env_float("INFLUX_RETRY_MAX", 30)

MQTT_USERNAME = read_secret(
    "MQTT_USERNAME_FILE",
    "/run/secrets/mqtt2influx_username",
)

MQTT_PASSWORD = read_secret(
    "MQTT_PASSWORD_FILE",
    "/run/secrets/mqtt2influx_password",
)

INFLUX_PASSWORD = read_secret(
    "INFLUX_PASSWORD_FILE",
    "/run/secrets/influx_mqtt2influx_password",
)


class DBWriterThread(Thread):
    def __init__(self, influx_client, *args, **kwargs):
        self.influx_client = influx_client
        self.data_queue = deque()
        self.queue_lock = Lock()
        super().__init__(*args, **kwargs)

    def schedule_item(self, client, device_id, control_id, value):
        event_time = (
            datetime.now(timezone.utc)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z")
        )
        item = (
            event_time,
            client,
            device_id,
            control_id,
            value,
        )

        with self.queue_lock:
            self.data_queue.append(item)

    def queue_size(self):
        with self.queue_lock:
            return len(self.data_queue)

    def get_items(self, mininterval, maxitems):
        started = time.monotonic()
        items = []

        while (
            time.monotonic() - started < mininterval
            and len(items) < maxitems
        ):
            item = None

            with self.queue_lock:
                if self.data_queue:
                    item = self.data_queue.popleft()

            if item is None:
                time.sleep(mininterval * 0.1)
            else:
                items.append(item)

        return items

    def write_with_retry(self, db_req_body, item_count, client_count):
        delay = max(0.1, INFLUX_RETRY_INITIAL)

        while True:
            try:
                ok = self.influx_client.write_points(db_req_body)
                if ok is False:
                    raise RuntimeError(
                        "InfluxDB write_points returned False"
                    )

                logging.info(
                    "Wrote %d items for %d clients",
                    item_count,
                    client_count,
                )
                return
            except Exception:
                logging.exception(
                    "InfluxDB write failed; "
                    "batch=%d queued=%d retry_in=%.1fs",
                    item_count,
                    self.queue_size(),
                    delay,
                )
                time.sleep(delay)
                delay = min(delay * 2, max(INFLUX_RETRY_MAX, 0.1))

    def run(self):
        while True:
            items = self.get_items(
                mininterval=0.05,
                maxitems=200,
            )

            db_req_body = []
            stat_clients = set()

            for (
                event_time,
                client,
                device_id,
                control_id,
                value,
            ) in items:
                serialized = self.serialize_data_item(
                    event_time,
                    client,
                    device_id,
                    control_id,
                    value,
                )

                if serialized:
                    db_req_body.append(serialized)
                    stat_clients.add(client)

            if db_req_body:
                logging.info(
                    "Prepared %d points from %d MQTT items "
                    "for %d clients; queued=%d",
                    len(db_req_body),
                    len(items),
                    len(stat_clients),
                    self.queue_size(),
                )

                # Preserve production behaviour.
                time.sleep(10)

                self.write_with_retry(
                    db_req_body,
                    len(db_req_body),
                    len(stat_clients),
                )

    @staticmethod
    def serialize_data_item(
        event_time,
        client,
        device_id,
        control_id,
        value,
    ):
        value = value.replace("\n", " ")

        if not value:
            return None

        fields = {}

        try:
            value_f = float(value)

            if not math.isnan(value_f):
                fields["value_f"] = value_f
        except ValueError:
            pass

        if "value_f" not in fields:
            fields["value_s"] = value

        return {
            "measurement": "mqtt_data",
            "time": event_time,
            "tags": {
                "client": client,
                "channel": f"{device_id}/{control_id}",
            },
            "fields": fields,
        }


db_writer_94 = None


def on_mqtt_message(client, userdata, msg):
    global db_writer_94

    if msg.retain:
        return

    if "bridge" in msg.topic:
        return

    if "homeassistant_" in msg.topic:
        logging.info(
            "Zigbee: topic=%s, value=%s",
            msg.topic,
            msg.payload.decode("utf8"),
        )

        device1 = msg.topic[msg.topic.find("/") + 1:]
        msg1 = msg.payload.decode("utf8")[1:-1]
        parts1 = msg1.split(",")

        for part1 in parts1:
            param1 = part1[1:part1.find(":") - 1]
            value1 = part1[part1.find(":") + 1:]

            try:
                db_writer_94.schedule_item(
                    "zigbee",
                    device1,
                    param1,
                    value1,
                )
            except Exception:
                logging.exception(
                    "Exception during schedule zigbee item 94"
                )

        return

    parts = msg.topic.split("/")
    mqtt_client = None

    if len(parts) < 4:
        return

    if parts[1] == "client":
        mqtt_client = parts[2]
        parts = parts[3:]

    if len(parts) != 4:
        return

    device_id = parts[1]
    control_id = parts[3]

    try:
        value = msg.payload.decode("utf8")
    except Exception:
        value = "Error during decoding"

    logging.info(
        "WB: topic=%s, value=%s",
        msg.topic,
        value,
    )

    if device_id == "wb-adc":
        return

    db_writer_94.schedule_item(
        mqtt_client,
        device_id,
        control_id,
        value,
    )


def on_mqtt_connect(client, userdata, flags, reason_code, properties):
    if reason_code != 0:
        logging.error(
            "MQTT connection failed: %s",
            reason_code,
        )
        return

    rc, _ = client.subscribe(MQTT_TOPIC)
    if rc != mqtt.MQTT_ERR_SUCCESS:
        logging.error(
            "MQTT subscribe failed: topic=%s rc=%s",
            MQTT_TOPIC,
            rc,
        )
        return

    logging.info(
        "MQTT connected and subscribed: topic=%s",
        MQTT_TOPIC,
    )


def on_mqtt_disconnect(
    client,
    userdata,
    disconnect_flags,
    reason_code,
    properties,
):
    if reason_code != 0:
        logging.warning(
            "MQTT disconnected unexpectedly: %s",
            reason_code,
        )


def main():
    global db_writer_94

    logging.info(
        "Starting mqtt2influx: MQTT=%s:%d, InfluxDB=%s:%d/%s",
        MQTT_HOST,
        MQTT_PORT,
        INFLUX_HOST,
        INFLUX_PORT,
        INFLUX_DATABASE,
    )

    influx_client = InfluxDBClient(
        INFLUX_HOST,
        INFLUX_PORT,
        username=INFLUX_USERNAME,
        password=INFLUX_PASSWORD,
        database=INFLUX_DATABASE,
        timeout=INFLUX_TIMEOUT,
    )

    db_writer_94 = DBWriterThread(
        influx_client,
        daemon=True,
    )
    db_writer_94.start()

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=None,
        clean_session=True,
        protocol=mqtt.MQTTv31,
    )

    client.username_pw_set(
        MQTT_USERNAME,
        MQTT_PASSWORD,
    )

    client.on_connect = on_mqtt_connect
    client.on_disconnect = on_mqtt_disconnect
    client.on_message = on_mqtt_message

    client.reconnect_delay_set(
        min_delay=1,
        max_delay=30,
    )

    client.connect_async(
        MQTT_HOST,
        MQTT_PORT,
    )

    return int(
        client.loop_forever(
            retry_first_connection=True,
        )
        or 0
    )


if __name__ == "__main__":
    sys.exit(main())
