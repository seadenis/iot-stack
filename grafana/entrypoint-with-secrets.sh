#!/bin/sh
set -eu

SECRET=/run/secrets/influx_grafana_password

if [ ! -r "$SECRET" ]; then
    echo "ERROR: Grafana InfluxDB password secret is not readable" >&2
    exit 1
fi

INFLUX_GRAFANA_PASSWORD="$(cat "$SECRET")"

if [ -z "$INFLUX_GRAFANA_PASSWORD" ]; then
    echo "ERROR: Grafana InfluxDB password secret is empty" >&2
    exit 1
fi

export INFLUX_GRAFANA_PASSWORD

exec /run.sh
