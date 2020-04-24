from obspy import read
from obspy import UTCDateTime
from obspy.clients.fdsn import Client

# copy of example script for fdsn web service retrieval on obspy pages
# avoids need for saving a data file with the repository
print("Retrieving ANMO seismogram with obspy fdsn web service client")
client = Client("IRIS")
t = UTCDateTime("2010-02-27T06:45:00.000")

d = client.get_waveforms("IU", "ANMO", "00", "LH*", t, t + 60 * 60)
# We need this for 3c test later, but we will load them now
#inventory=client.get_stations(network='IU',station='ANMO',starttime=t,endtime=t+3600.0)
# couldn't make that work for initial testing - hack temporary until I crack the 
# obspy documentation
# assume order x1,x2,x3

from mspasspy.ccore import MetadataDefinitions
from mspasspy.ccore import MDDefFormat
import mspasspy.io.converter

print("Loading MetadataDefinitions")
mdef=MetadataDefinitions("obspy_namespace.pf",MDDefFormat.PF)
print("success - setting up auxiliary metadata and alias testing")
tr=d[0]
mdother=["Ptime","invalid"]
aliases=["sta"]
tr.stats["Ptime"]=1000.0
print("Running Trace2TimeSeries converter for TimeSeries")
dout=tr.toTimeSeries(mdef,mdother,aliases)
print("Trace2TimeSeries completed but test generates a complaint stored in the log")
print("This is the messsage stored in the TimeSeries log as a complaint")
elog=dout.elog.get_error_log()
print(elog[0].message)
# Extension is needed here to test converter to Seismogram - under development
print("Starting test for 3C converter")
print("Trying simple test assuming data are cardinal (they aren't but ok for a test")
d3c=d.toSeismogram(mdef,cardinal=bool(1))
print("succeeded")
print("Trying conversion with auxiliary metadata and no errors")
d[0].stats["Ptime"]=1000.0
mdo2=["Ptime","hang","vang"]
# manually set hang and vang
d[0].stats["hang"]=90.0
d[0].stats["vang"]=90.0
d[1].stats["hang"]=1.0
d[1].stats["vang"]=90.0
d[2].stats["hang"]=0.0
d[2].stats["vang"]=0.0
d3c=d.toSeismogram(mdef,mdo2,aliases,azimuth="hang",dip="vang")
print("Success - now trying to add an invalid key to copy ")
mdo2.append("bad")
d3c=d.toSeismogram(mdef,mdo2,aliases,azimuth="hang",dip="vang")
print("Stream2Seismogram completed but test generates a complaint stored in the log")
print("This is the messsage stored in the TimeSeries log as a complaint")
elog=dout.elog.get_error_log()
print(elog[0].message)

