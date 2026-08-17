#!/bin/sh
set -eu

INFLUX_PW_FILE="/run/secrets/kapacitor_influx_password"
MQTT_PW_FILE="/run/secrets/kapacitor_mqtt_password"

if [ ! -s "$INFLUX_PW_FILE" ]; then
    echo "ERROR: missing or empty $INFLUX_PW_FILE" >&2
    exit 1
fi

if [ ! -s "$MQTT_PW_FILE" ]; then
    echo "ERROR: missing or empty $MQTT_PW_FILE" >&2
    exit 1
fi

export KAPACITOR_INFLUXDB_0_PASSWORD="$(cat "$INFLUX_PW_FILE")"
export KAPACITOR_MQTT_0_PASSWORD="$(cat "$MQTT_PW_FILE")"

exec kapacitord -config /etc/kapacitor/kapacitor.conf
