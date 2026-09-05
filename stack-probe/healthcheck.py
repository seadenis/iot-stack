#!/usr/bin/env python3
import sys
import time
from pathlib import Path

p = Path("/tmp/stack-probe.heartbeat")
try:
    age = time.time() - p.stat().st_mtime
except OSError:
    sys.exit(1)

sys.exit(0 if age < 180 else 1)
