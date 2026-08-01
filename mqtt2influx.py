#!/usr/bin/python3

import logging
import math
import os
import sys
import time
from collections import deque
from pathlib import Path
from threading import Thread

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

MQTT_USERNAME = read_secret(
    "MQTT_USERNAME_FILE",
    "/run/secrets/mqtt2influx_username",
)

MQTT_PASSWORD = read_secret(
    "MQTT_PASSWORD_FILE",
    "/run/secrets/mqtt2influx_password",
)


class DBWriterThread(Thread):
    def __init__(self, influx_client, *args, **kwargs):
        self.influx_client = influx_client
        self.data_queue = deque()
        super().__init__(*args, **kwargs)

    def schedule_item(self, client, device_id, control_id, value):
        item = (client, device_id, control_id, value)
        self.data_queue.append(item)

    def get_items(self, mininterval, maxitems):
        started = time.time()
        items = []

        while (
            time.time() - started < mininterval
            and len(items) < maxitems
        ):
            try:
                item = self.data_queue.popleft()
            except IndexError:
                time.sleep(mininterval * 0.1)
            else:
                items.append(item)

        return items

    def run(self):
        while True:
            items = self.get_items(
                mininterval=0.05,
                maxitems=200,
            )

            db_req_body = []
            stat_clients = set()

            for client, device_id, control_id, value in items:
                serialized = self.serialize_data_item(
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
                    "Write %d items for %d clients",
                    len(items),
                    len(stat_clients),
                )

                # Preserve production behaviour.
                time.sleep(10)

                try:
                    self.influx_client.write_points(db_req_body)
                except Exception:
                    logging.exception(
                        "Exception during writing points"
                    )

    @staticmethod
    def serialize_data_item(
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
        database=INFLUX_DATABASE,
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

    client.on_message = on_mqtt_message

    client.connect(
        MQTT_HOST,
        MQTT_PORT,
    )

    client.subscribe(MQTT_TOPIC)

    while True:
        rc = client.loop()

        if rc != mqtt.MQTT_ERR_SUCCESS:
            logging.error(
                "MQTT loop stopped with return code %s",
                rc,
            )
            return int(rc)


if __name__ == "__main__":
    sys.exit(main())
