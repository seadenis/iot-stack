#!/bin/sh
set -eu

read_secret() {
    file="$1"
    name="$2"

    if [ ! -s "$file" ]; then
        echo "Missing or empty secret: ${name}" >&2
        exit 1
    fi

    value="$(cat "$file")"

    if [ -z "$value" ]; then
        echo "Empty secret: ${name}" >&2
        exit 1
    fi

    printf '%s' "$value"
}

MONITORING_MQTT_PASSWORD="$(
    read_secret \
        /run/secrets/monitoring_mqtt_ingest_password \
        monitoring_mqtt_ingest_password
)"

INFLUX_MONITORING_PASSWORD="$(
    read_secret \
        /run/secrets/influx_monitoring_password \
        influx_monitoring_password
)"

export MONITORING_MQTT_PASSWORD
export INFLUX_MONITORING_PASSWORD

exec telegraf \
    --config /etc/telegraf/telegraf.conf
