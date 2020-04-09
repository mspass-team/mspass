#!/usr/bin/env python3
# I still haven't fixed the pythonpath mess so I need this to make this
# script run.  For the test suite it should not be required and in fact
# must not be required to make sure imports work correctly for the package
import sys
sys.path.append('/home/pavlis/src/mspass/python')

from pymongo import MongoClient
from obspy import read_inventory
from obspy import read_events
from mspasspy.db import save_inventory
from mspasspy.db import save_catalog
from mspasspy.db import load_stations
from mspasspy.db import load_inventory
from mspasspy.db import load_event
from mspasspy.db import load_catalog

client=MongoClient()
db=client['testing']
# turning this test program into a testsuite program that could be run
# automatically requires changing this file and the read_events file below.
# The one's I used were produced from a test using obspy's mass downloader
# that I'm checking into this same test directory.   The same data I'm using
# can be obtained by running testbulk.py. The file name here will need to change
inv=read_inventory('/home/pavlis/testing/obspy_downloading/sitexml/*.xml')
[count,ntotal]=save_inventory(db,inv)
print("dbave_inventory wrote ",count," documents of ",
      ntotal," processed")
stats=db.command('dbstats')
print('dbstats')
print(stats)
nsite=db.site.count_documents({})
print('Size of site collection=',nsite)
# This file name will also need to be changed
cat=read_events('/home/pavlis/testing/obspy_downloading/events/2004-2005.xml')
count=save_catalog(db,cat)
print('save_catalog wrote ',count,' entries in source collection')
print('dbstat now')
stats=db.command('dbstats')
print(stats)
nsourcenow=db.source.count_documents({})
print('number of documents in source collection=',nsourcenow)
#
# Done with save function testing - now try to load the same data with
# various perturbations - includes standard cases but the tests below are not
# exhaustive and do not fully test for error conditions
#
sta='C15'
net='YX'
x=load_stations(db.site,net,sta)
print("Trying load_stations with defaulted parameters")
type(x)
for a in x:
    print(a['sta'],a['net'])
print("trying with loc set")
loc=''
x=load_stations(db.site,net,sta,loc)
for a in x:
    print(a['sta'],a['net'])
print('trying load_inventory with all defaults')
inv=load_inventory(db)
print(inv)
print('trying load_inventory for one net code')
inv=load_inventory(db,net='Z9')
print(inv)
print('trying load_inventory for a single station')
inv=load_inventory(db,sta='C14',net='YX')
print(inv)
print('trying query on single loc code')
inv=load_inventory(db,loc='00')
print(inv)
print('Testing source collection query functions')
dbsource=db.source
print('trying query by source_id')
s=load_event(dbsource,source_id=2000)
print(s['source_id'],s['source_lat'],s['source_lon'],s['source_depth'])
print('trying query from same source by objectid')
oid=s['_id']
oids=str(oid)
s=load_event(dbsource,oidstr=oids)
print(s['source_id'],s['source_lat'],s['source_lon'],s['source_depth'])
print('Trying load_catalog of full collection (default)')
cat=load_catalog(dbsource)
print(cat)
print('trying to load events with source depth > 100 km')
catsubset=load_catalog(dbsource,depth={'$gt' : 100.0})
print(catsubset)
