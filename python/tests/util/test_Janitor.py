#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 14:33:17 2024

@author: pavlis
"""
#from mspasspy.util.Janitor import Janitor
from Janitor import Janitor
from helper import (
    get_live_seismogram,
    get_live_timeseries,
    get_live_timeseries_ensemble,
    get_live_seismogram_ensemble,
)
from mspasspy.ccore.seismic import (
    TimeSeries,
    Seismogram,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
)
from mspasspy.ccore.utility import MsPASSError
import pytest

def test_Janitor_constructor():
    # test default yaml creating from yaml file
    j1 = Janitor()
    assert len(j1.TimeSeries_keepers) > 0
    assert len(j1.Seismogram_keepers) > 0
    # test override kwargs 
    keylist = ["foo","bar","npts"]
    j2 = Janitor(TimeSeries_keepers=keylist)
    kl = j2.TimeSeries_keepers
    assert len(kl)==3
    for k in keylist: 
        assert k in kl
    j2 = Janitor(Seismogram_keepers=keylist)
    kl = j2.Seismogram_keepers
    assert len(kl)==3
    for k in keylist: 
        assert k in kl
        
def generic_methods_tester_atomic(t):
    """
    Generic function used to test methods on all supported data types. 
    input t must be a type object that resolves to one of:
    TimeSeries or Seismogram
    """
    janitor = Janitor()
    # use isinstance to get a working object to run test upon
    if t == TimeSeries:
        d = get_live_timeseries()
        d0 = TimeSeries(d)
    elif t == Seismogram:
        d = get_live_seismogram()
        d0 = Seismogram(d)
    else:
        raise ValueError("illegal value for arg0={}".t)
    k0 = d.keys()
    # this case should do nothing
    # note intentionally don't copy d because can assume same as initial
    d = janitor.clean(d)
    for k in d.keys():
        assert k in k0
    # Add an attribute that should be deleted
    d["foo"] = "bar"
    d = janitor.clean(d)
    kcleaned = d.keys()
    assert len(kcleaned) == len(k0)
    for k in d.keys():
        assert k in k0
        
    # similar tests for collect_trash
    d["foo"] = "bar"
    trash = janitor.collect_trash(d)
    assert "foo" in trash
    assert trash["foo"] == "bar"
    # collect_trash should also be equivalent to clean of d
    for k in d.keys():
        assert k in k0
    
    # test bag trash method which puts the trash in a dictionary 
    # with "trash" as the key to the data removed from d
    d["foo"] = "bar"
    d = janitor.bag_trash(d)
    assert "trash" in d
    x = d["trash"]
    assert isinstance(x,dict)
    assert x["foo"] == "bar"
    
    # test add2keepers
    # here we need a fresh new timeseries in d
    janitor.add2keepers("Ptime")
    if t==TimeSeries:
        d = get_live_timeseries()
    elif t==Seismogram:
        d = get_live_seismogram()
    d['Ptime'] = 10.0
    d["foo"] = "bar"
    d = janitor.bag_trash(d)
    assert d["Ptime"] == 10.0
    assert "trash" in d
    x=d["trash"]
    assert len(x) == 1
    assert x["foo"] == "bar"
        
    
def generic_methods_tester_ensembles(t):
    """
    Generic function used to test methods on all supported data types. 
    input t must be a type object that resolves to one of:
    TimeSeries or Seismogram
    """
    # first test behavior on ensemble Metadata
    janitor = Janitor(process_ensemble_members=False)
    # use isinstance to get a working object to run test upon
    if t == TimeSeriesEnsemble:
        d = get_live_timeseries_ensemble(3)
        d0 = TimeSeriesEnsemble(d)
    elif t == SeismogramEnsemble:
        d = get_live_seismogram_ensemble(3)
        d0 = SeismogramEnsemble(d)
    else:
        raise ValueError("illegal value for arg0={}".t)
    k0 = d.keys()
    # this case should do nothing
    # note intentionally don't copy d because can assume same as initial
    d = janitor.clean(d)
    for k in d.keys():
        assert k in k0
    # Add an attribute that should be deleted
    d["foo"] = "bar"
    d = janitor.clean(d)
    kcleaned = d.keys()
    assert len(kcleaned) == len(k0)
    for k in d.keys():
        assert k in k0
        
    # similar tests for collect_trash
    d["foo"] = "bar"
    trash = janitor.collect_trash(d)
    assert "foo" in trash
    assert trash["foo"] == "bar"
    # collect_trash should also be equivalent to clean of d
    for k in d.keys():
        assert k in k0
    
    # test bag trash method which puts the trash in a dictionary 
    # with "trash" as the key to the data removed from d
    d["foo"] = "bar"
    d = janitor.bag_trash(d)
    assert "ensemble_trash" in d
    x = d["ensemble_trash"]
    assert isinstance(x,dict)
    assert x["foo"] == "bar"
    
    # test add2keepers
    # here we need a fresh new timeseries in d
    janitor.add2keepers("Ptime","ensemble")
    if t==TimeSeriesEnsemble:
        d = TimeSeriesEnsemble(d0)
    elif t==SeismogramEnsemble:
        d = SeismogramEnsemble(d0)
    d['Ptime'] = 10.0
    d["foo"] = "bar"
    d = janitor.bag_trash(d)
    assert d["Ptime"] == 10.0
    assert "ensemble_trash" in d
    x=d["ensemble_trash"]
    assert len(x) == 1
    assert x["foo"] == "bar"
    # add a value to the keepers for ensemble - default is empty
    janitor.add2keepers("source_id","ensemble")
    d['source_id'] = 'a source id place holder'
    d = janitor.clean(d)
    assert len(d.keys())==2
    assert "source_id" in d
    assert "Ptime" in d
    # now repeat but applying to members instead of enemble container.
def test_janitor_methods():
    generic_methods_tester_atomic(TimeSeries)
    generic_methods_tester_atomic(Seismogram)
    generic_methods_tester_ensembles(TimeSeriesEnsemble)
    generic_methods_tester_ensembles(SeismogramEnsemble)
def test_janitor_error_handlers():
    with pytest.raises(MsPASSError,match="Cannnot open keepers_file"):
        j = Janitor(keepers_file="bad_file_name")

#test_Janitor_constructor()
#test_janitor_methods()
#test_janitor_error_handlers()
