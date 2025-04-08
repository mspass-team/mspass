#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 09:39:23 2025

@author: pavlis
"""

from launcher import HPCClusterLauncher
launcher=HPCClusterLauncher("configuration.yaml",verbose=True,auto_launch=False)
launcher.launch()
launcher.run("/N/scratch/pavlis/testing/test.py")
#launcher.interactive_session()
launcher.shutdown()
