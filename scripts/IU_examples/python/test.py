#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb  2 07:56:32 2025

@author: pavlis
"""


def add2(t):
    return t+2.0

import dask.bag as dbg
import numpy as np
from mspasspy.ccore.seismic import TimeSeries
print("hello world")
print("Got that far - try creating a TieSeries object")
y=TimeSeries()
print("Success")
import mspasspy.client as msc
print("Trying to instantiate mspass_client")
mspass_client = msc.Client()
print("Client created - trying to get db handle")
db = mspass_client.get_database("test")
print("db handle successfully created")
doc={"foo" : 1, "bar" : 2.0}
db.testing.insert_one(doc)
print("insert succeeded - trying to retrieve")
doc2 = db.testing.find_one()
print(doc)
N=1000
x = np.ones(N)
print("Trying to run a simple dask operation")
d = dbg.from_sequence(x)
d = d.map(add2)
d.compute()
print("Finished")
print(d)
