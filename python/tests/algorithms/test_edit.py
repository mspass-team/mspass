import numpy as np
from mspasspy.ccore.seismic import TimeSeries, TimeSeriesEnsemble
from mspasspy.algorithms.edit import (
    MetadataGT,
    MetadataGE,
    MetadataLT,
    MetadataLE,
    MetadataEQ,
    MetadataNE,
    MetadataDefined,
    MetadataUndefined,
    MetadataInterval,
    FiringSquad,
)
from mspasspy.algorithms.edit import (
    SetValue,
    ChangeKey,
    Add,
    Subtract,
    Multiply,
    Divide,
    IntegerDivide,
    Mod,
    Add2,
    Subtract2,
    Multiply2,
    Divide2,
    IntegerDivide2,
    Mod2,
    MetadataOperatorChain,
    erase_metadata,
)


def test_edit():
    d = TimeSeries(100)
    d.set_live()
    d["test_int"] = 2
    d["test_float"] = 4.0

    # Test ensemble is 5 copies of d
    ens = TimeSeriesEnsemble(5)
    for i in range(5):
        ens.member.append(d)
    ens.set_live()

    int_tester = MetadataGT("test_int", 1, verbose=True)
    x = int_tester.kill_if_true(d)
    assert not x.live

    # test with ensemble
    enscpy = TimeSeriesEnsemble(ens)
    enscpy = int_tester.kill_if_true(enscpy, apply_to_members=True)
    for i in range(len(enscpy.member)):
        assert enscpy.member[i].dead()

    d.set_live()
    int_tester = MetadataGT("test_int", 3, verbose=True)
    x = int_tester.kill_if_true(d)
    assert x.live
    enscpy = TimeSeriesEnsemble(ens)
    enscpy = int_tester.kill_if_true(enscpy, apply_to_members=True)
    for i in range(len(enscpy.member)):
        assert enscpy.member[i].live

    real_tester = MetadataGT("test_float", 1.0, verbose=True)
    x = real_tester.kill_if_true(d)
    assert x.dead()
    d.set_live()
    real_tester = MetadataGT("test_float", 10.0, verbose=True)
    x = real_tester.kill_if_true(d)
    assert x.live

    # Same for GE
    real_tester = MetadataGE("test_float", 1.0, verbose=True)
    x = real_tester.kill_if_true(d)
    assert x.dead()
    enscpy = TimeSeriesEnsemble(ens)
    enscpy = real_tester.kill_if_true(enscpy, apply_to_members=True)
    for i in range(len(enscpy.member)):
        assert enscpy.member[i].dead()
    d.set_live()
    real_tester = MetadataGE("test_float", 10.0, verbose=True)
    x = real_tester.kill_if_true(d)

    d.set_live()
    int_testerGE = MetadataGE("test_int", 2)
    x = int_testerGE.kill_if_true(d)
    assert d.dead()
    enscpy = TimeSeriesEnsemble(ens)
    enscpy = int_testerGE.kill_if_true(enscpy, apply_to_members=True)
    for i in range(len(enscpy.member)):
        assert enscpy.member[i].dead()

    # equality tester
    d.set_live()
    eqtest = MetadataEQ("test_int", 2, verbose=True)
    x = eqtest.kill_if_true(d)
    assert x.dead()
    enscpy = TimeSeriesEnsemble(ens)
    enscpy = eqtest.kill_if_true(enscpy, apply_to_members=True)
    for i in range(len(enscpy.member)):
        assert enscpy.member[i].dead()

    d.set_live()
    eqtest = MetadataEQ("test_int", 9, verbose=True)
    x = eqtest.kill_if_true(d)
    assert x.live

    # non equality tester
    d.set_live()
    eqtest = MetadataNE("test_int", 9, verbose=True)
    x = eqtest.kill_if_true(d)
    assert x.dead()
    enscpy = TimeSeriesEnsemble(ens)
    enscpy = eqtest.kill_if_true(enscpy, apply_to_members=True)
    for i in range(len(enscpy.member)):
        assert enscpy.member[i].dead()

    d.set_live()
    eqtest = MetadataNE("test_int", 2, verbose=True)
    x = eqtest.kill_if_true(d)
    assert x.live

    # LT versions
    d.set_live()
    lttest = MetadataLT("test_int", 3, verbose=True)
    x = lttest.kill_if_true(d)
    assert x.dead()
    enscpy = TimeSeriesEnsemble(ens)
    enscpy = lttest.kill_if_true(enscpy, apply_to_members=True)
    for i in range(len(enscpy.member)):
        assert enscpy.member[i].dead()

    d.set_live()
    lttest = MetadataLT("test_int", 1, verbose=True)
    x = lttest.kill_if_true(d)
    assert x.live

    # LE version
    d.set_live()
    letest = MetadataLE("test_int", 3, verbose=True)
    x = letest.kill_if_true(d)
    assert x.dead()

    enscpy = TimeSeriesEnsemble(ens)
    enscpy = letest.kill_if_true(enscpy, apply_to_members=True)
    for i in range(len(enscpy.member)):
        assert enscpy.member[i].dead()

    d.set_live()
    letest = MetadataLE("test_int", 1, verbose=True)
    x = letest.kill_if_true(d)
    assert x.live
    d.set_live()

    letest = MetadataLE("test_int", 2, verbose=True)
    x = letest.kill_if_true(d)
    assert x.dead()

    enscpy = TimeSeriesEnsemble(ens)
    enscpy = letest.kill_if_true(enscpy, apply_to_members=True)
    for i in range(len(enscpy.member)):
        assert enscpy.member[i].dead()

    # test MetadataInterval - first a value inside
    tester = MetadataInterval("test_int", 1, 3, verbose=True)
    d.set_live()
    x = tester.kill_if_true(d)
    assert x.live

    d.set_live()
    tester.kill_if_outside = False
    x = tester.kill_if_true(d)
    assert x.dead()

    enscpy = TimeSeriesEnsemble(ens)
    enscpy = tester.kill_if_true(enscpy, apply_to_members=True)
    for i in range(len(enscpy.member)):
        assert enscpy.member[i].dead()

    # now test a value at an edge
    tester = MetadataInterval("test_float", 4.0, 8.0, verbose=True)
    d.set_live()
    x = tester.kill_if_true(d)
    assert x.live  # not killed because of equality with 4.0 so not "inside"

    tester.kill_if_outside = False
    x = tester.kill_if_true(d)
    assert x.dead()

    enscpy = TimeSeriesEnsemble(ens)
    enscpy = tester.kill_if_true(enscpy, apply_to_members=True)
    for i in range(len(enscpy.member)):
        assert enscpy.member[i].dead()

    # now test switch to remove equality as true
    tester.use_lower_edge = False
    tester.kill_if_outside = False
    d.set_live()
    x = tester.kill_if_true(d)
    assert x.live

    d.set_live()
    tester.kill_if_outside = True
    x = tester.kill_if_true(d)
    assert x.dead()

    enscpy = TimeSeriesEnsemble(ens)
    enscpy = tester.kill_if_true(enscpy, apply_to_members=True)
    for i in range(len(enscpy.member)):
        assert enscpy.member[i].dead()

    # Existence testers
    d.set_live()
    tester = MetadataDefined("test_float", verbose=True)
    tester.kill_if_true(d)
    assert d.dead()

    enscpy = TimeSeriesEnsemble(ens)
    enscpy = tester.kill_if_true(enscpy, apply_to_members=True)
    for i in range(len(enscpy.member)):
        assert enscpy.member[i].dead()

    d.set_live()
    tester = MetadataUndefined("foo", verbose=True)
    tester.kill_if_true(d)
    assert d.dead()

    mob = [int_tester, real_tester]
    fstest = FiringSquad(mob)
    d.set_live()
    x = fstest.kill_if_true(d)
    assert x.live

    addon_tester = MetadataGT("test_float", 1.0, verbose=True)
    fstest += addon_tester
    d.set_live()
    x = fstest.kill_if_true(d)
    assert x.dead()

    enscpy = TimeSeriesEnsemble(ens)
    enscpy = addon_tester.kill_if_true(enscpy, apply_to_members=True)
    for i in range(len(enscpy.member)):
        assert enscpy.member[i].dead()

    d2 = TimeSeries(d)
    d2.set_live()

    op = SetValue("a", 5)
    d2 = op.apply(d2)
    assert d2["a"] == 5

    enscpy = TimeSeriesEnsemble(ens)
    enscpy = op.apply(enscpy)
    assert enscpy["a"] == 5
    enscpy = op.apply(enscpy, apply_to_members=True)
    for d in enscpy.member:
        assert d["a"] == 5

    op = ChangeKey("a", "b")
    d2 = op.apply(d2)
    assert d2["b"] == 5
    enscpy = op.apply(enscpy)
    assert enscpy["b"] == 5
    enscpy = op.apply(enscpy, apply_to_members=True)
    for d in enscpy.member:
        assert d["b"] == 5

    # Add
    chnlist = ["test_float", "b"]
    d2 = erase_metadata(d2, chnlist)
    assert not d2.is_defined("test_float")
    assert not d2.is_defined("b")

    d2 = TimeSeries(d)
    enscpy = TimeSeriesEnsemble(ens)
    op = Add("test_float", 2.5)
    d2 = op.apply(d2)
    assert np.isclose(d2["test_float"], 6.5)
    # use a different value for test_float for the ensemble metadata

    enscpy = op.apply(enscpy, apply_to_members=True)
    for x in enscpy.member:
        assert np.isclose(x["test_float"], 6.5)

    enscpy["a"] = 8.0
    ensop = Add("a", 2.2)
    enscpy = ensop.apply(enscpy)
    assert np.isclose(enscpy["a"], 10.2)

    # Subtract
    d2 = TimeSeries(d)
    enscpy = TimeSeriesEnsemble(ens)
    op = Subtract("test_float", 2.5)
    d2 = op.apply(d2)
    assert np.isclose(d2["test_float"], 1.5)
    # use a different value for test_float for the ensemble metadata

    enscpy = op.apply(enscpy, apply_to_members=True)
    for x in enscpy.member:
        assert np.isclose(x["test_float"], 1.5)

    enscpy["a"] = 8.0
    ensop = Subtract("a", 2.2)
    enscpy = ensop.apply(enscpy)
    assert np.isclose(enscpy["a"], 5.8)

    # Multiply
    d2 = TimeSeries(d)
    enscpy = TimeSeriesEnsemble(ens)
    op = Multiply("test_float", 2.5)
    d2 = op.apply(d2)
    assert np.isclose(d2["test_float"], 10.0)
    # use a different value for test_float for the ensemble metadata

    enscpy = op.apply(enscpy, apply_to_members=True)
    for x in enscpy.member:
        assert np.isclose(x["test_float"], 10.0)

    enscpy["a"] = 8.0
    ensop = Multiply("a", 2)
    enscpy = ensop.apply(enscpy)
    assert np.isclose(enscpy["a"], 16.0)

    # Divide
    d2 = TimeSeries(d)
    enscpy = TimeSeriesEnsemble(ens)
    op = Divide("test_float", 8.0)
    d2 = op.apply(d2)
    assert np.isclose(d2["test_float"], 0.5)
    # use a different value for test_float for the ensemble metadata

    enscpy = op.apply(enscpy, apply_to_members=True)
    for x in enscpy.member:
        assert np.isclose(x["test_float"], 0.5)

    enscpy["a"] = 8.0
    ensop = Subtract("a", 2.0)
    enscpy = ensop.apply(enscpy)
    assert np.isclose(enscpy["a"], 6.0)

    # IntegerDivide
    d2 = TimeSeries(d)
    enscpy = TimeSeriesEnsemble(ens)
    op = IntegerDivide("test_float", 3.0)
    d2 = op.apply(d2)
    assert np.isclose(d2["test_float"], 1.0)
    # use a different value for test_float for the ensemble metadata

    enscpy = op.apply(enscpy, apply_to_members=True)
    for x in enscpy.member:
        assert np.isclose(x["test_float"], 1.0)

    enscpy["a"] = 9
    ensop = IntegerDivide("a", 2)
    enscpy = ensop.apply(enscpy)
    assert enscpy["a"] == 4

    # Mod
    d2 = TimeSeries(d)
    enscpy = TimeSeriesEnsemble(ens)
    op = Mod("test_float", 3.0)
    d2 = op.apply(d2)
    assert np.isclose(d2["test_float"], 1.0)
    # use a different value for test_float for the ensemble metadata

    enscpy = op.apply(enscpy, apply_to_members=True)
    for x in enscpy.member:
        assert np.isclose(x["test_float"], 1.0)

    enscpy["a"] = 9
    ensop = Mod("a", 2)
    enscpy = ensop.apply(enscpy)
    assert enscpy["a"] == 1

    # Test of binary operators
    # Add2
    d2 = TimeSeries(d)
    enscpy = TimeSeriesEnsemble(ens)
    op = Add2("opout", "test_float", "test_int")
    d2 = op.apply(d)
    assert np.isclose(d["opout"], 6.0)
    enscpy["a"] = 9
    enscpy["b"] = 3
    ensop = Add2("enopout", "a", "b")
    enscpy = ensop.apply(enscpy)
    assert np.isclose(enscpy["enopout"], 12)
    enscpy = op.apply(enscpy, apply_to_members=True)
    for d in enscpy.member:
        assert np.isclose(d["opout"], 6.0)

    # Sutract2
    d2 = TimeSeries(d)
    enscpy = TimeSeriesEnsemble(ens)
    op = Subtract2("opout", "test_float", "test_int")
    d2 = op.apply(d)
    assert np.isclose(d["opout"], 2.0)
    enscpy["a"] = 9
    enscpy["b"] = 3
    ensop = Subtract2("enopout", "a", "b")
    enscpy = ensop.apply(enscpy)
    assert np.isclose(enscpy["enopout"], 6)
    enscpy = op.apply(enscpy, apply_to_members=True)
    for d in enscpy.member:
        assert np.isclose(d["opout"], 2.0)

    # Multiply2
    d2 = TimeSeries(d)
    enscpy = TimeSeriesEnsemble(ens)
    op = Multiply2("opout", "test_float", "test_int")
    d2 = op.apply(d)
    assert np.isclose(d["opout"], 8.0)
    enscpy["a"] = 9
    enscpy["b"] = 3
    ensop = Multiply2("enopout", "a", "b")
    enscpy = ensop.apply(enscpy)
    assert np.isclose(enscpy["enopout"], 27)
    enscpy = op.apply(enscpy, apply_to_members=True)
    for d in enscpy.member:
        assert np.isclose(d["opout"], 8.0)

    # Divide2
    d2 = TimeSeries(d)
    enscpy = TimeSeriesEnsemble(ens)
    op = Divide2("opout", "test_float", "test_int")
    d2 = op.apply(d)
    assert np.isclose(d["opout"], 2.0)
    enscpy["a"] = 9
    enscpy["b"] = 3
    ensop = Divide2("enopout", "a", "b")
    enscpy = ensop.apply(enscpy)
    assert np.isclose(enscpy["enopout"], 3)
    enscpy = op.apply(enscpy, apply_to_members=True)
    for d in enscpy.member:
        assert np.isclose(d["opout"], 2.0)

    # IntegerDivide2
    d2 = TimeSeries(d)
    enscpy = TimeSeriesEnsemble(ens)
    op = IntegerDivide2("opout", "test_float", "test_int")
    d2 = op.apply(d)
    assert np.isclose(d["opout"], 4.0 // 2.0)
    enscpy["a"] = 9
    enscpy["b"] = 3
    ensop = IntegerDivide2("enopout", "a", "b")
    enscpy = ensop.apply(enscpy)
    assert np.isclose(enscpy["enopout"], 9 // 3)
    enscpy = op.apply(enscpy, apply_to_members=True)
    for d in enscpy.member:
        assert np.isclose(d["opout"], 4.0 // 2.0)

    # Mod2
    d2 = TimeSeries(d)
    enscpy = TimeSeriesEnsemble(ens)
    op = Mod2("opout", "test_float", "test_int")
    d2 = op.apply(d)
    assert np.isclose(d["opout"], 4.0 % 2.0)
    enscpy["a"] = 9
    enscpy["b"] = 3
    ensop = Mod2("enopout", "a", "b")
    enscpy = ensop.apply(enscpy)
    assert np.isclose(enscpy["enopout"], 9 % 3)
    enscpy = op.apply(enscpy, apply_to_members=True)
    for d in enscpy.member:
        assert np.isclose(d["opout"], 4.0 % 2.0)

    # Test operator chain
    d2 = TimeSeries(d)
    enscpy = TimeSeriesEnsemble(ens)

    # atomic operators to use in the chain
    op1 = Add2("lhs", "test_float", "test_int")
    op2 = Multiply("lhs", 4.0)
    op3 = Divide2("lhs", "lhs", "test_int")
    oplist = [op1, op2, op3]
    opchain = MetadataOperatorChain(oplist)
    d2 = opchain.apply(d2)
    assert np.isclose(d2["lhs"], 12.0)

    enscpy = opchain.apply(enscpy, apply_to_members=True)
    for d in enscpy.member:
        assert np.isclose(d["lhs"], 12.0)

    # Test to change the chain a bit for the ensemble metadata - a detail
    # of how this implemented makes the result more robust
    enscpy["enstest"] = 5
    enscpy["enstest_float"] = 2.5
    op1 = Add2("lhs", "enstest", "enstest_float")
    op3 = Divide("lhs", 3)
    oplist = [op1, op2, op3]
    opchain = MetadataOperatorChain(oplist)
    enscpy = opchain.apply(enscpy)
    assert np.isclose(enscpy["lhs"], (4 * 7.5) / 3)
