from obspy import read
# copy of example script for fdsn web service retrieval on obspy pages
# avoids need for saving a data file with the repository
print("Retrieving ANMO seismogram with obspy fdsn web service client")
from obspy import UTCDateTime
from obspy.clients.fdsn import Client
client = Client("IRIS")
t = UTCDateTime("2010-02-27T06:45:00.000")
d = client.get_waveforms("IU", "ANMO", "00", "LHZ", t, t + 60 * 60)
from mspasspy import MetadataDefinitions
from mspasspy import MDDefFormat
print("Loading MetadataDefinitions")
mdef=MetadataDefinitions("obspy_namespace.pf",MDDefFormat.PF)
print("success - setting up auxiliary metadata and alias testing")
tr=d[0]
mdother=["Ptime","invalid"]
aliases=["sta"]
tr.stats["Ptime"]=1000.0
print("Running obspy2mspass converter for TimeSeries")
from obspy2mspass import obspy2mspass
dout=obspy2mspass(tr,mdef,mdother,aliases)
print("obspy2mspass completed but test generates a complaint stored in the log")
print("This is the messsage stored in the TimeSeries log as a complaint")
elog=dout.get_error_log()
print(elog[0].message)
print("Done with test of obspy2mspass for TimeSeries")
# Extension is needed here to test converter to Seismogram - under development
