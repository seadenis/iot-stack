FROM python:3.12.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY python-ingest/requirements.txt /tmp/requirements.txt

RUN pip install \
      --no-cache-dir \
      -r /tmp/requirements.txt \
    && rm -f /tmp/requirements.txt

COPY mqtt2influx.py /app/mqtt2influx.py

CMD ["python", "/app/mqtt2influx.py"]
