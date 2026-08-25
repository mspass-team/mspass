#!/usr/bin/env python3
"""Start GeoLab services before the original JupyterHub single-user server."""

import os
import sys


bootstrap = "/usr/sbin/start-mspass-geolab.sh"
singleuser = os.path.realpath(__file__) + ".mspass-original"
os.execv(bootstrap, [bootstrap, sys.executable, singleuser, *sys.argv[1:]])
