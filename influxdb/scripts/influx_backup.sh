#!/bin/bash
set -euo pipefail
NOW=$(date +%F_%H-%M)
BACKUP_DIR="/opt/iot-stack/influxdb/backup"
FILE="$BACKUP_DIR/influx_${NOW}.tar.gz"

GOMAXPROCS=1 docker compose exec influxdb influxd backup -portable /backup/influx_${NOW}

tar -C "$BACKUP_DIR" -czf "$FILE" "influx_${NOW}"
rm -rf "$BACKUP_DIR/influx_${NOW}"

# загрузка на Google Drive (remote «gdrive» уже настроен через rclone config)
rclone copy "$FILE" iot-influxdb:influx-backup
# чистим локальные копии >14 дней
find "$BACKUP_DIR" -type f -mtime +14 -delete
# чистим в GDrive всё, что старше 60 дней
rclone delete iot-influxdb:influx-backup --min-age 60d
