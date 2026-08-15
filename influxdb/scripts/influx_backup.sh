#!/bin/bash
set -euo pipefail

NOW=$(date +%F_%H-%M)
BACKUP_DIR="/opt/iot-stack/influxdb/backup"
NAME="influx_${NOW}.tar.gz"
FILE="$BACKUP_DIR/$NAME"
SHA_FILE="$FILE.sha256"
REMOTE="iot-influxdb:influx-backup"

mkdir -p "$BACKUP_DIR"

echo "=== INFLUX BACKUP START ==="
echo "timestamp=$(date --iso-8601=seconds)"
echo "archive=$FILE"

GOMAXPROCS=1 docker compose exec influxdb \
    influxd backup -portable "/backup/influx_${NOW}"

tar -C "$BACKUP_DIR" \
    -czf "$FILE" \
    "influx_${NOW}"

rm -rf "$BACKUP_DIR/influx_${NOW}"

echo
echo "=== LOCAL ARCHIVE VERIFY ==="

gzip -t "$FILE"

(
    cd "$BACKUP_DIR"
    sha256sum "$NAME" > "$NAME.sha256"
    sha256sum -c "$NAME.sha256"
)

echo
echo "=== UPLOAD TO GOOGLE DRIVE ==="

rclone copy "$FILE" "$REMOTE"
rclone copy "$SHA_FILE" "$REMOTE"

echo
echo "=== REMOTE VERIFY ==="

REMOTE_SHA="$(
    rclone cat "$REMOTE/$NAME.sha256" |
    awk 'NF {print $1; exit}'
)"

LOCAL_SHA="$(
    sha256sum "$FILE" |
    awk '{print $1}'
)"

test -n "$REMOTE_SHA"
test "$LOCAL_SHA" = "$REMOTE_SHA"

echo "remote_sha256=VERIFIED"

echo
echo "=== RETENTION ==="

find "$BACKUP_DIR" \
    -type f \
    -mtime +14 \
    -delete

rclone delete "$REMOTE" \
    --min-age 60d

echo
echo "=== INFLUX BACKUP COMPLETE ==="
echo "archive=$NAME"
echo "sha256=$LOCAL_SHA"
echo "timestamp=$(date --iso-8601=seconds)"
