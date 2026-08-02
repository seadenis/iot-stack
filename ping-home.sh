#!/bin/bash
set -euo pipefail

ping -c 5 -q -W 2 185.246.193.65 |
awk -F'[ /]' '/^rtt/{v=substr($8,1,length($8)-2)} END{print (v ? v : 10000)}' |
XDG_CONFIG_HOME=/opt/iot-stack/secrets/ping-home-client \
xargs mosquitto_pub \
  -t "/client/wb_AMTXYWW3/devices/pi/controls/RTT_Home" \
  -m
