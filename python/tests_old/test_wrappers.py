import mspasspy.ccore
import math
import matplotlib.pyplot as plt
import numpy as np

print('//////////////////Testing of Metadata Related Components///////////')
pf=mspasspy.ccore.AntelopePf('test_md.pf')
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
mdl=mspasspy.ccore.get_mdlist(pf,'mdlist')
print('MedadataList returned by get_mdlist=',mdl)
print('///////////Starting tests of TimeSeries /////////////////')
ts=mspasspy.ccore.CoreTimeSeries()
ts=mspasspy.ccore.CoreTimeSeries(4)
ts.ns=100
ts.t0=0.0
ts.dt=0.001
ts.live=1
# This is how an enum class sets a value
ts.tref=mspasspy.ccore.TimeReferenceType.Relative
ts.s.append(1.0)
ts.s.append(2.0)
ts.s.append(3.0)
ts.s.append(4.0)
print('Test samples with append=',ts.s)
for i in range(8) :
    ts.s[i]=i*0.5
print('Test using indexing operator=',ts.s)
ts=mspasspy.ccore.CoreTimeSeries(100)
# makes a sine function with approximately one cycle
for i in range(100) :
    ts.s[i]=math.sin(6.28*i*0.01)
#plot routine only handles np array objects so we have to copy for this tests
a=np.zeros(100)
for i in range(100) :
    a[i]=ts.s[i]
# remove these comments if running interactively.  Not consistent
# with automated tests on github
#plt.plot(a)
#plt.show()
print('contents of sine wave vector')
print(ts.s)
print('///////////Starting tests of Seismogram /////////////////')
seis=mspasspy.ccore.CoreSeismogram()
seis=mspasspy.ccore.CoreSeismogram(100)
#a=np.arange(15).reshape(3,5)
#a=np.array([[1,2,3],[4,5,6],[7,8,9],[10,11,12]],np.double)
#a=np.array([1,2,3,4,5,6,7,8,9,10,11,12],np.double)
a=np.zeros((3,6),dtype=np.double,order='F')
ii=1
for i in range(3):
    for j in range(6):
        a[i,j]=ii
        ii = ii + 1
print("3x4 data matrix")
print(a)
seis.u=mspasspy.ccore.dmatrix(a)
print("component 0 data")
for i in range(4):
    print(seis.u[0,i])
print("component 1 data")
for i in range(4):
    print(seis.u[1,i])
print("component 2 data")
for i in range(4):
    print(seis.u[2,i])
#mddef=mspasspy.ccore.MetadataDefinitions('obspy_namespace.pf',PF)
print("Test completed successfully")
