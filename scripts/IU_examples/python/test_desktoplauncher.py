#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 17 05:32:17 2025

@author: pavlis
"""

from launcher import DesktopLauncher
dltest=DesktopLauncher("configuration_docker.yaml")
x=input("Hit return to continue")
dltest.shutdown()
