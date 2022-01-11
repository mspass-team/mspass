from mspasspy.ccore.seismic import TimeSeries
from mspasspy.algorithms.edit import (MetadataGT,
            MetadataGE,
            MetadataLT,
            MetadataLE,
            MetadataEQ,
            MetadataNE,
            MetadataDefined,
            MetadataUndefined,
            MetadataInterval,
            FiringSquad)


d=TimeSeries(100)
d.set_live()
d['test_int'] = 2
d['test_float'] =4.0

int_tester = MetadataGT('test_int',1,verbose=True)
x=int_tester.kill_if_true(d)
assert not x.live
d.set_live()
int_tester = MetadataGT('test_int',3,verbose=True)
x=int_tester.kill_if_true(d)
assert x.live

real_tester = MetadataGT('test_float',1.0,verbose=True)
x=real_tester.kill_if_true(d)
assert x.dead()
d.set_live()
real_tester = MetadataGT('test_float',10.0,verbose=True)
x=real_tester.kill_if_true(d)
assert x.live

# Same for GE
real_tester = MetadataGE('test_float',1.0,verbose=True)
x=real_tester.kill_if_true(d)
assert x.dead()
d.set_live()
real_tester = MetadataGE('test_float',10.0,verbose=True)
x=real_tester.kill_if_true(d)

d.set_live()
int_testerGE = MetadataGE('test_int',2)
x = int_testerGE.kill_if_true(d)
assert d.dead()

#equality tester
d.set_live()
eqtest = MetadataEQ('test_int',2,verbose=True)
x=eqtest.kill_if_true(d)
assert x.dead()

d.set_live()
eqtest = MetadataEQ('test_int',9,verbose=True)
x=eqtest.kill_if_true(d)
assert x.live

# LT versions
d.set_live()
lttest = MetadataLT('test_int',3,verbose=True)
x=lttest.kill_if_true(d)
assert x.dead()
d.set_live()
lttest = MetadataLT('test_int',1,verbose=True)
x=lttest.kill_if_true(d)
assert x.live

#LE version
d.set_live()
letest = MetadataLE('test_int',3,verbose=True)
x=letest.kill_if_true(d)
assert x.dead()
d.set_live()
letest = MetadataLE('test_int',1,verbose=True)
x=letest.kill_if_true(d)
assert x.live
d.set_live()
letest = MetadataLE('test_int',2,verbose=True)
x=letest.kill_if_true(d)
assert x.dead()


# test MetadataInterval - first a value inside
tester = MetadataInterval('test_int',1,3,verbose=True)
d.set_live()
x = tester.kill_if_true(d)
assert x.live
d.set_live()
tester.kill_if_outside = False
x = tester.kill_if_true(d)
assert x.dead()
# now test a value at an edge
tester = MetadataInterval('test_float',4.0,8.0,verbose=True)
d.set_live()
x = tester.kill_if_true(d)
assert x.live  # not killed because of equality with 4.0 so not "inside"
tester.kill_if_outside = False
x = tester.kill_if_true(d)
assert x.dead()

# now test switch to remove equality as true 
tester.use_lower_edge=False
tester.kill_if_outside= False
d.set_live()
x = tester.kill_if_true(d)
assert x.live
d.set_live()
tester.kill_if_outside = True
x = tester.kill_if_true(d)
assert x.dead()

# Existence testers
d.set_live()
tester = MetadataDefined("test_float",verbose=True)
tester.kill_if_true(d)
assert d.dead()

d.set_live()
tester = MetadataUndefined("foo",verbose=True)
tester.kill_if_true(d)
assert d.dead()

mob = [int_tester,real_tester]
fstest = FiringSquad(mob)
d.set_live()
x=fstest.kill_if_true(d)
assert x.live
addon_tester = MetadataGT('test_float',1.0,verbose=True)
fstest += addon_tester
d.set_live()
x=fstest.kill_if_true(d)
assert not x.live


