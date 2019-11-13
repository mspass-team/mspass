import mspasspy as msp
import math
import matplotlib.pyplot as plt
import numpy as np
print('//////////////////Testing of Metadata Related Components///////////')
pf=msp.AntelopePf('test_md.pf')
k=pf.keys()
print('simple parameter keys found=',k)
print('trying put methods')
pf.put('testreal',2.0)
r=pf.get_double('testreal')
print('put 2.0 get of same=',r)
pf.put('testint',4)
i=pf.get_int('testint')
print('put 4 get of same=',i)
pf.put('teststring','foo')
s=pf.get_string('teststring')
print('put string foo get of same=',s)
print('keys after put test=',pf.keys())
print('trying get_tbl')
tk=pf.tbl_keys()
ak=pf.arr_keys()
print('keys for Tbls in this file=',tk)
print('keys for Arrs in this file=',ak)
tbl=pf.get_tbl('mdlist')
print('Extracted this:  ',tbl)
print('trying get_branch')
md=pf.get_branch('test_nested_tag')
print('keys in extracted map=',md.keys())
print('testing downcast of AntelopePf to Metadata')
md2=pf.ConvertToMetadata()
print('type of result=',type(md2))
print('key of result=',md2.keys())
mdl=msp.get_mdlist(pf,'mdlist')
print('MedadataList returned by get_mdlist=',mdl)
print('///////////Starting tests of TimeSeries /////////////////')
ts=msp.CoreTimeSeries()
ts=msp.CoreTimeSeries(4)
ts.ns=100
ts.t0=0.0
ts.dt=0.001
#ts.tref=TimeReferenceType.Relative
ts.s.append(1.0)
ts.s.append(2.0)
ts.s.append(3.0)
ts.s.append(4.0)
print('Test samples with append=',ts.s)
for i in range(0,8) :
    ts.s[i]=i*0.5
print('Test counting samples=',ts.s)
ts=msp.CoreTimeSeries()
# Rookie in python -there is likely a more concise way to do this with sin
# makes a sine function with approximately one cycle
for i in range(0,100) :
    ts.s.append(math.sin(6.28*i*0.01))
plt.plot(ts.s)
print('contents of sine wave vector')
print(ts.s)
print('///////////Starting tests of Seismogram /////////////////')
seis=msp.CoreSeismogram()
seis=msp.CoreSeismogram(100)
#a=np.arange(15).reshape(3,5)
a=np.array([[1,2,3],[4,5,6],[7,8,9]],np.double)
print(a)
u=msp.dmatrix(a)
seis.u=u
print(seis.u)
