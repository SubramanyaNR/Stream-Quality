#!/usr/bin/env python3
"""Run one SQL statement as the admin user (used by the shell test scripts, e.g. TRUNCATE)."""
import sys

import pgutil

pgutil.execute(sys.argv[1])
