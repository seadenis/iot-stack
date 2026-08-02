#!/usr/bin/env python3
"""
Parse an InfluxQL `COUNT()` dump and print one summary line
per MQTT-channel in the form:

    <channel_name> <count>

Usage:
    python mqtt_count_summary.py dump.txt               # read from file
    cat dump.txt | python mqtt_count_summary.py -       # read from stdin
"""
import re
import sys
from pathlib import Path
from typing import TextIO


# -------- helpers -----------------------------------------------------------
TAG_RE   = re.compile(r"^tags:\s+channel=(.+)$")
COUNT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}.*?\s+(\d+)\s*$")


def parse(f: TextIO):
    """Yield (channel, count) tuples from an InfluxQL COUNT() dump."""
    channel = None

    for line in f:
        tag_match = TAG_RE.match(line.strip())
        if tag_match:                                     # tags: channel=...
            channel = tag_match.group(1)
            continue

        cnt_match = COUNT_RE.match(line.strip())
        if cnt_match and channel is not None:            # … timestamp  <count>
            yield channel, int(cnt_match.group(1))
            channel = None                               # reset for next block


# -------- main ---------------------------------------------------------------
def main():
    src = sys.argv[1] if len(sys.argv) > 1 else "-"
    with (open(src) if src != "-" else sys.stdin) as fp:
        for chan, cnt in parse(fp):
            print(f"{chan}#{cnt}")


if __name__ == "__main__":
    main()
