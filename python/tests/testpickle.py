import sys
sys.path.append('/home/pavlis/src/mspass/python')
from mspasspy.ccore import CoreSeismogram
d=CoreSeismogram(200)
d.put_double('delta',1.0)
d.put_double('dt',1.0)
d.put('npts',200)
d.ns=200
d.t0=100.0
d.live=True
from mspasspy.ccore import Seismogram
d2=Seismogram(d,'invalid')
import pickle
x=pickle.dumps(d2)
print("pickle succeeded")
print("trying to restore")
d3=pickle.loads(x)
print("pickle loads completed")
print("data npts=",d.get_int('npts'))
print('same stored in struct of BasicTimeSeries=',d.ns)
print('data t0=',d.t0)
print('Now testing pickle for TimeSeries data')
from mspasspy.ccore import TimeSeries
from mspasspy.ccore import CoreTimeSeries
d0=CoreTimeSeries(500)
d0.live=True
d0.dt=1.0
d0.ns=500
d=TimeSeries(d0,'invalid')
s=pickle.dumps(d)
print('Pickle dumps succeeded')
print('size of string returned by pickle=',len(s))
print('Trying loads')
dr=pickle.loads(s)
print('Finished - BasicTimeSeries attributes in output')
print('ns=',dr.ns)
print('dt=',dr.dt)
print('t0=',dr.t0)

