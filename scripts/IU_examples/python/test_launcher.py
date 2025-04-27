#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 09:39:23 2025

@author: pavlis
"""

from launcher import HPCClusterLauncher
import time
t0=time.time()
launcher=HPCClusterLauncher("configuration.yaml",verbose=True,auto_launch=False)
t1=time.time()
print("time to instantiate launcher=",t1-t0)
launcher.launch(verbose=True)
t2=time.time()
print("Time to launch all services=",t2-t1)
time.sleep(15)
t3=time.time()
launcher.run("/N/scratch/pavlis/testing/test.py")
t4=time.time()
print("Run time=",t4-t3)
#launcher.interactive_session()
launcher.shutdown()
