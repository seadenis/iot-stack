#!/bin/sh
set -eu

SECRET_DST="/tmp/stack-probe-secrets"

mkdir -p "$SECRET_DST"
chmod 700 "$SECRET_DST"

for name in \
    mqtt2influx_username \
    mqtt2influx_password \
    influx_monitoring_password \
    influx_grafana_password
do
    src="/run/secrets/$name"
    dst="$SECRET_DST/$name"

    if [ ! -s "$src" ]; then
        echo "Missing or empty secret: $src" >&2
        exit 1
    fi

    cp "$src" "$dst"
    chmod 400 "$dst"
    chown probe:probe "$dst"
done

chown probe:probe "$SECRET_DST"

export STACK_PROBE_SECRET_DIR="$SECRET_DST"

exec su-exec probe "$@"
