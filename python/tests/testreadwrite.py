#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test program for MongoDB readers ands writers
Created on Thu Jan 23 07:10:28 2020

@author: pavlis
"""

import pymongo
from bson.objectid import ObjectId
from pymongo import MongoClient
from mspasspy.mongodb.io import dict2md
from mspasspy.mongodb.io import dbload3C
from mspasspy.mongodb.io import dbsave3C
from mspasspy.ccore import MongoDBConverter
from mspasspy.ccore import Seismogram
from mspasspy.ccore import Metadata
from mspasspy.ccore import MetadataDefinitions
from mspasspy.ccore import ErrorLogger
print('trying to construct MetadataDefinitions')
mdef=MetadataDefinitions()
elog=ErrorLogger()
client=MongoClient()
db=client['2005']
col=db.wf
a=col.find_one()
print('running dict2md')
md=dict2md(a,mdef,elog)
# hack fix for current situation - not needed if I fixed the db
md.put_string('dir','/home/pavlis/src/mspass/python/mspasspy/importer')
print('Calling Seismogram constructor')
d=Seismogram(md)
d.elog=elog
d.clear_modified()
print('Trying constructor for MongoDBConverter')
mc=MongoDBConverter()
print('original waveform object id string=',d.get_string('wfid_string'))
print('Starting dbsave3C function')
iret=dbsave3C(db,d,mc,mmode='save',smode='gridfs')
print('dbsave3C returned error count=',iret)
print("dbsave returned error count=",iret)
print('object id string on return from dbsave=',d.get_string('wfid_string'))
oidstr=d.get_string('gridfs_idstr')
print('id string for new gridfs file=',oidstr)
wfid=d.get_string('wfid_string')
print('id of wf collection record=',wfid)
oid=ObjectId(wfid)
print('trying to read the data just written back')
dc=dbload3C(db,oid,mdef)
print('trying an update with one change to sta attribute')
d.put_string('sta','FOO')
iret=dbsave3C(db,d,mc,mmode='updatemd',smode='gridfs')
print('dbsave3C returned error count=',iret)
print('object id string on return from dbsave=',d.get_string('wfid_string'))
oidstr=d.get_string('gridfs_idstr')
print('id string for new gridfs file=',oidstr)
from mspasspy.ccore import agc
d2=agc(d,10.0)
# now try an update with these data
iret=dbsave3C(db,d,mc,mmode='updateall',smode='gridfs')
