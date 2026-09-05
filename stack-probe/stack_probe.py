#!/usr/bin/env python3
import json
import logging
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)

INTERVAL = int(os.getenv("PROBE_INTERVAL", "60"))
CANARY_INTERVAL = int(os.getenv("CANARY_INTERVAL", "60"))
MOSQUITTO_RECOVERY_GRACE = int(
    os.getenv("MOSQUITTO_RECOVERY_GRACE", "120")
)
ONESHOT = os.getenv("ONESHOT", "0").lower() in ("1", "true", "yes")
MQTT2INFLUX_TIMEOUT = int(os.getenv("MQTT2INFLUX_TIMEOUT", "25"))
TM_MAX_AGE = int(os.getenv("TELEGRAF_MONITORING_MAX_AGE", "120"))
TM_MQTT_ACTIVITY_WINDOW = int(
    os.getenv("TELEGRAF_MONITORING_MQTT_ACTIVITY_WINDOW", "180")
)

INFLUX = os.getenv("INFLUX_URL", "http://influxdb:8086")
GRAFANA = os.getenv("GRAFANA_URL", "http://grafana:3000")
KAPACITOR = os.getenv("KAPACITOR_URL", "http://kapacitor:9092")
MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
PUBLIC_HOST = os.getenv("PUBLIC_HOST", "dansu.ru")
SNAPSHOT_TOPIC = os.getenv(
    "SNAPSHOT_TOPIC",
    "dansu/alerts/monitoring/internal_stack_state",
)

HEARTBEAT = Path("/tmp/stack-probe.heartbeat")


SECRET_DIR = Path(
    os.getenv("STACK_PROBE_SECRET_DIR", "/run/secrets")
)


def secret(name):
    value = (SECRET_DIR / name).read_text().strip()
    if not value:
        raise RuntimeError(f"empty secret: {name}")
    return value


MQTT_USER = secret("mqtt2influx_username")
MQTT_PASS = secret("mqtt2influx_password")
MONITORING_PASS = secret("influx_monitoring_password")
GRAFANA_PASS = secret("influx_grafana_password")


def run(cmd, *, input_text=None, timeout=10, check=True):
    try:
        p = subprocess.run(
            cmd,
            input=input_text,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        # Never include the command line here:
        # MQTT credentials are passed as command arguments.
        raise RuntimeError(
            f"command timed out after {timeout}s"
        ) from None

    if check and p.returncode != 0:
        raise RuntimeError(
            f"rc={p.returncode} stderr={p.stderr.strip()[:300]}"
        )
    return p


def res(component, check, started, ok=True, **extra):
    item = {
        "component": component,
        "check": check,
        "ok": bool(ok),
        "latency_ms": round((time.monotonic() - started) * 1000, 1),
    }
    item.update(extra)
    return item


def influx_query(db, q):
    p = run([
        "curl", "-fsS",
        "--max-time", "5",
        "-u", f"grafana_ro:{GRAFANA_PASS}",
        "--get", f"{INFLUX}/query",
        "--data-urlencode", f"db={db}",
        "--data-urlencode", f"q={q}",
    ])
    payload = json.loads(p.stdout)
    if payload.get("results") and payload["results"][0].get("error"):
        raise RuntimeError(payload["results"][0]["error"])
    return payload


def influx_write(lines):
    run([
        "curl", "-fsS",
        "--max-time", "5",
        "-u", f"monitoring_wr:{MONITORING_PASS}",
        "-X", "POST",
        "--data-binary", "@-",
        f"{INFLUX}/write?db=monitoring&rp=raw_180d&precision=ns",
    ], input_text=lines)


def values(payload):
    series = payload.get("results", [{}])[0].get("series", [])
    if not series:
        return []
    return series[0].get("values", [])


def check_influx_ping():
    t = time.monotonic()
    try:
        p = run([
            "curl", "-sS", "-o", "/dev/null",
            "--max-time", "5",
            "-w", "%{http_code}",
            f"{INFLUX}/ping",
        ])
        return res("influxdb", "ping", t, p.stdout == "204",
                   status_code=int(p.stdout or 0))
    except Exception as e:
        return res("influxdb", "ping", t, False, detail=str(e))


def check_influx_write_read():
    t = time.monotonic()
    nonce = uuid.uuid4().hex
    try:
        influx_write(
            f'dansu_stack_canary,probe=stack-probe '
            f'nonce="{nonce}",value=1i'
        )
        v = values(influx_query(
            "monitoring",
            'SELECT LAST("nonce") FROM '
            '"raw_180d"."dansu_stack_canary" '
            'WHERE "probe"=\'stack-probe\'',
        ))
        ok = bool(v and len(v[0]) >= 2 and v[0][1] == nonce)
        return res("influxdb", "write_read", t, ok)
    except Exception as e:
        return res("influxdb", "write_read", t, False, detail=str(e))


def mqtt_auth():
    return [
        "-h", MQTT_HOST,
        "-p", "1883",
        "-u", MQTT_USER,
        "-P", MQTT_PASS,
    ]


def mqtt_publish(topic, payload, retain=False):
    cmd = ["mosquitto_pub", *mqtt_auth(), "-q", "1", "-t", topic, "-m", payload]
    if retain:
        cmd.append("-r")
    run(cmd, timeout=8)


def check_mosquitto_roundtrip():
    t = time.monotonic()
    token = uuid.uuid4().hex
    topic = f"monitoring/dansu/internal/probe/roundtrip/{token}"
    sub = subprocess.Popen(
        [
            "mosquitto_sub", *mqtt_auth(),
            "-q", "1", "-t", topic,
            "-C", "1", "-W", "8",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        time.sleep(0.3)
        mqtt_publish(topic, token)
        out, err = sub.communicate(timeout=9)
        if sub.returncode != 0:
            raise RuntimeError(f"subscriber rc={sub.returncode}: {err.strip()}")
        return res("mosquitto", "pubsub_roundtrip", t, out.strip() == token)
    except Exception as e:
        sub.kill()
        return res("mosquitto", "pubsub_roundtrip", t, False, detail=str(e))


def check_telegraf_mqtt():
    t = time.monotonic()
    try:
        p = run([
            "mosquitto_sub", *mqtt_auth(),
            "-t", "system",
            "-C", "1", "-W", "15",
            "-v",
        ], timeout=16)
        topic = p.stdout.split(" ", 1)[0].strip()
        return res("telegraf", "mqtt_freshness", t, bool(topic), detail=topic)
    except Exception as e:
        return res("telegraf", "mqtt_freshness", t, False, detail=str(e))


def check_mqtt2influx():
    t = time.monotonic()
    nonce = "dansu-" + uuid.uuid4().hex
    try:
        mqtt_publish("probe/stack_probe/canary/value", nonce)
        deadline = time.monotonic() + MQTT2INFLUX_TIMEOUT
        while time.monotonic() < deadline:
            v = values(influx_query(
                "mqtt_data",
                'SELECT LAST("value_s") FROM "mqtt_data" '
                'WHERE "channel"=\'stack_probe/value\' '
                'AND time > now() - 2m',
            ))
            if v and len(v[0]) >= 2 and v[0][1] == nonce:
                return res("mqtt2influx", "mqtt_to_influx_canary", t, True)
            time.sleep(1)
        raise RuntimeError(
            f"canary absent after {MQTT2INFLUX_TIMEOUT}s"
        )
    except Exception as e:
        return res(
            "mqtt2influx", "mqtt_to_influx_canary", t, False, detail=str(e)
        )


def check_nginx():
    t = time.monotonic()
    try:
        p = run([
            "curl", "-sS",
            "--max-time", "8",
            "--connect-to", f"{PUBLIC_HOST}:443:nginx:443",
            "-o", "/dev/null",
            "-w", "%{http_code}|%{redirect_url}",
            f"https://{PUBLIC_HOST}/",
        ])
        code, location = p.stdout.split("|", 1)
        ok = code == "302" and "/public-dashboards/" in location
        return res(
            "nginx", "https_frontdoor", t, ok,
            status_code=int(code), detail=location
        )
    except Exception as e:
        return res("nginx", "https_frontdoor", t, False, detail=str(e))


def check_grafana_health():
    t = time.monotonic()
    try:
        p = run([
            "curl", "-fsS",
            "--max-time", "5",
            f"{GRAFANA}/api/health",
        ])
        j = json.loads(p.stdout)
        ok = j.get("database") == "ok"
        return res(
            "grafana", "api_health", t, ok,
            detail=f"database={j.get('database')} version={j.get('version')}"
        )
    except Exception as e:
        return res("grafana", "api_health", t, False, detail=str(e))


def check_grafana_public():
    t = time.monotonic()
    try:
        p = run([
            "curl", "-fsSL",
            "--max-time", "10",
            "--max-redirs", "3",
            "--connect-to", f"{PUBLIC_HOST}:443:nginx:443",
            f"https://{PUBLIC_HOST}/",
        ], timeout=12)
        ok = "grafanaBootData" in p.stdout
        return res("grafana", "public_dashboard", t, ok)
    except Exception as e:
        return res("grafana", "public_dashboard", t, False, detail=str(e))


def check_kapacitor():
    t = time.monotonic()
    try:
        p = run([
            "curl", "-fsS",
            "--max-time", "5",
            f"{KAPACITOR}/kapacitor/v1/tasks"
            "?fields=status&fields=executing&fields=error&limit=100",
        ])
        tasks = json.loads(p.stdout).get("tasks", [])
        enabled = [x for x in tasks if x.get("status") == "enabled"]
        bad = [
            x.get("id", "?")
            for x in enabled
            if not x.get("executing") or x.get("error")
        ]
        ok = bool(enabled) and not bad
        return res(
            "kapacitor", "enabled_tasks", t, ok,
            detail=f"enabled={len(enabled)} bad={','.join(bad) if bad else 'none'}"
        )
    except Exception as e:
        return res("kapacitor", "enabled_tasks", t, False, detail=str(e))


def check_telegraf_monitoring():
    t = time.monotonic()
    try:
        v = values(influx_query(
            "monitoring",
            'SELECT LAST("metrics_written") FROM '
            '"raw_180d"."dansu_tm_internal_agent"',
        ))
        if not v:
            raise RuntimeError("no dansu_tm_internal_agent points")
        ts = datetime.fromisoformat(v[0][0].replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - ts).total_seconds()
        return res(
            "telegraf-monitoring", "influx_freshness", t,
            age <= TM_MAX_AGE,
            age_s=round(age, 1),
        )
    except Exception as e:
        return res(
            "telegraf-monitoring", "influx_freshness", t, False, detail=str(e)
        )


def check_telegraf_monitoring_mqtt_activity():
    t = time.monotonic()
    try:
        v = values(influx_query(
            "monitoring",
            'SELECT FIRST("messages_received") AS "first", '
            'LAST("messages_received") AS "last", '
            'COUNT("messages_received") AS "samples" '
            'FROM "raw_180d"."dansu_tm_internal_mqtt_consumer" '
            f'WHERE time > now() - {TM_MQTT_ACTIVITY_WINDOW}s',
        ))

        if not v or len(v[0]) < 4:
            raise RuntimeError(
                "no mqtt_consumer activity samples in "
                f"{TM_MQTT_ACTIVITY_WINDOW}s"
            )

        first = v[0][1]
        last = v[0][2]
        samples = int(v[0][3] or 0)

        if first is None or last is None:
            raise RuntimeError(
                "mqtt_consumer activity counters are empty"
            )

        first = int(first)
        last = int(last)

        # A counter reset is also activity, therefore inequality is enough.
        ok = samples >= 2 and first != last

        return res(
            "telegraf-monitoring",
            "mqtt_input_activity",
            t,
            ok,
            first_value=first,
            last_value=last,
            samples=samples,
            detail=(
                f"window={TM_MQTT_ACTIVITY_WINDOW}s "
                f"first={first} last={last} samples={samples}"
            ),
        )
    except Exception as e:
        return res(
            "telegraf-monitoring",
            "mqtt_input_activity",
            t,
            False,
            detail=str(e),
        )


def esc_tag(v):
    return str(v).replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,")


def esc_str(v):
    return str(v).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def write_results(items):
    ts = time.time_ns()
    lines = []
    for x in items:
        f = [
            f"ok={1 if x['ok'] else 0}i",
            f"latency_ms={float(x['latency_ms'])}",
        ]
        if "age_s" in x:
            f.append(f"age_s={float(x['age_s'])}")
        if "status_code" in x:
            f.append(f"status_code={int(x['status_code'])}i")
        if "first_value" in x:
            f.append(f"first_value={int(x['first_value'])}i")
        if "last_value" in x:
            f.append(f"last_value={int(x['last_value'])}i")
        if "samples" in x:
            f.append(f"samples={int(x['samples'])}i")
        if x.get("detail"):
            f.append(f'detail="{esc_str(x["detail"])[:500]}"')
        lines.append(
            "dansu_stack_check,"
            f"component={esc_tag(x['component'])},"
            f"check={esc_tag(x['check'])} "
            + ",".join(f)
            + f" {ts}"
        )
    influx_write("\n".join(lines))


def snapshot(items):
    components = {}
    for x in items:
        c = components.setdefault(x["component"], {"ok": True, "checks": {}})
        check_data = {
            "ok": x["ok"],
            "latency_ms": x["latency_ms"],
        }

        for key in (
            "age_s",
            "detail",
            "suppressed",
            "raw_ok",
            "first_value",
            "last_value",
            "samples",
        ):
            if key in x:
                check_data[key] = x[key]

        c["checks"][x["check"]] = check_data
        c["ok"] = c["ok"] and x["ok"]

    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "overall": "OK" if all(x["ok"] for x in components.values()) else "FAIL",
        "components": components,
    }
    mqtt_publish(
        SNAPSHOT_TOPIC,
        json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
        retain=True,
    )


MQTT_DEPENDENT_CHECKS = {
    ("telegraf", "mqtt_freshness"),
    ("mqtt2influx", "mqtt_to_influx_canary"),
    ("telegraf-monitoring", "mqtt_input_activity"),
}


def main():
    logging.info(
        "Starting stack-probe interval=%ss canary_interval=%ss "
        "mosquitto_recovery_grace=%ss tm_mqtt_activity_window=%ss",
        INTERVAL,
        CANARY_INTERVAL,
        MOSQUITTO_RECOVERY_GRACE,
        TM_MQTT_ACTIVITY_WINDOW,
    )

    last_canary = None
    last_canary_at = 0.0

    mosquitto_prev_ok = None
    mosquitto_grace_until = 0.0

    while True:
        cycle = time.monotonic()
        items = []

        for fn in (
            check_influx_ping,
            check_influx_write_read,
            check_mosquitto_roundtrip,
            check_telegraf_mqtt,
            check_nginx,
            check_grafana_health,
            check_grafana_public,
            check_kapacitor,
            check_telegraf_monitoring,
            check_telegraf_monitoring_mqtt_activity,
        ):
            x = fn()
            items.append(x)
            logging.info(
                "%s/%s=%s %.1fms%s",
                x["component"], x["check"],
                "OK" if x["ok"] else "FAIL",
                x["latency_ms"],
                f" detail={x.get('detail')}" if x.get("detail") else "",
            )

        if last_canary is None or time.monotonic() - last_canary_at >= CANARY_INTERVAL:
            last_canary = check_mqtt2influx()
            last_canary_at = time.monotonic()
            logging.info(
                "%s/%s=%s %.1fms%s",
                last_canary["component"], last_canary["check"],
                "OK" if last_canary["ok"] else "FAIL",
                last_canary["latency_ms"],
                f" detail={last_canary.get('detail')}"
                if last_canary.get("detail") else "",
            )
        items.append(last_canary)

        # Track Mosquitto recovery separately from raw measurements.
        # Raw FAIL results are still written to InfluxDB, but transient
        # failures of MQTT-dependent services are suppressed in the
        # alerting snapshot for a short recovery window.
        mosquitto_result = next(
            (
                x for x in items
                if x["component"] == "mosquitto"
                and x["check"] == "pubsub_roundtrip"
            ),
            None,
        )

        now_mono = time.monotonic()

        if mosquitto_result is not None:
            mosquitto_ok = bool(mosquitto_result["ok"])

            if mosquitto_prev_ok is False and mosquitto_ok:
                mosquitto_grace_until = (
                    now_mono + MOSQUITTO_RECOVERY_GRACE
                )
                logging.warning(
                    "Mosquitto recovered; dependent recovery grace=%ss",
                    MOSQUITTO_RECOVERY_GRACE,
                )

            if not mosquitto_ok:
                mosquitto_grace_until = 0.0

            mosquitto_prev_ok = mosquitto_ok

        snapshot_items = [dict(x) for x in items]

        if now_mono < mosquitto_grace_until:
            grace_left = max(
                0,
                int(mosquitto_grace_until - now_mono),
            )

            for x in snapshot_items:
                if (
                    (x["component"], x["check"]) in MQTT_DEPENDENT_CHECKS
                    and not x["ok"]
                ):
                    raw_detail = x.get("detail", "FAIL")
                    x["raw_ok"] = False
                    x["ok"] = True
                    x["suppressed"] = True
                    x["detail"] = (
                        "suppressed during Mosquitto recovery grace; "
                        f"{grace_left}s remaining; raw={raw_detail}"
                    )

                    logging.warning(
                        "%s/%s raw FAIL suppressed during "
                        "Mosquitto recovery grace (%ss remaining)",
                        x["component"],
                        x["check"],
                        grace_left,
                    )

        try:
            write_results(items)
        except Exception:
            logging.exception("Could not write probe metrics to InfluxDB")

        try:
            snapshot(snapshot_items)
        except Exception:
            logging.exception("Could not publish retained MQTT snapshot")

        HEARTBEAT.touch()

        elapsed = time.monotonic() - cycle

        if ONESHOT:
            logging.info("Cycle complete %.1fs; one-shot complete", elapsed)
            return

        sleep_for = max(1.0, INTERVAL - elapsed)
        logging.info("Cycle complete %.1fs; sleep %.1fs", elapsed, sleep_for)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()

