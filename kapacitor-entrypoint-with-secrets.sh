#!/bin/sh
set -eu

PW_FILE="/run/secrets/kapacitor_influx_password"

if [ ! -s "$PW_FILE" ]; then
    echo "ERROR: missing or empty $PW_FILE" >&2
    exit 1
fi

export KAPACITOR_INFLUXDB_0_PASSWORD="$(cat "$PW_FILE")"

exec kapacitord -config /etc/kapacitor/kapacitor.conf
