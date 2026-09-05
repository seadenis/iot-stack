FROM python:3.12-alpine

RUN apk add --no-cache \
        ca-certificates \
        curl \
        mosquitto-clients \
        su-exec \
    && addgroup -S probe \
    && adduser -S -D -H -G probe probe

WORKDIR /app

COPY stack_probe.py /app/stack_probe.py
COPY healthcheck.py /app/healthcheck.py
COPY entrypoint.sh /usr/local/bin/stack-probe-entrypoint.sh

RUN chown -R probe:probe /app \
    && chmod 755 /usr/local/bin/stack-probe-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/stack-probe-entrypoint.sh"]

CMD ["python", "-u", "/app/stack_probe.py"]
