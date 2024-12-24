#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import pickle
import numpy as np
import pytest

# module to test
sys.path.append("python/tests")
from helper import (
    get_live_seismogram,
    get_live_timeseries,
    get_sin_timeseries,
    get_live_timeseries_ensemble,
    get_live_seismogram_ensemble,
)
from mspasspy.algorithms.window import WindowData
from mspasspy.algorithms.RFdeconProcessor import RFdeconProcessor, RFdecon
from mspasspy.ccore.seismic import Seismogram
from mspasspy.algorithms.basic import ExtractComponent
from mspasspy.util.seismic import print_metadata

def prediction_error_norm(ao,io):
    """
    Used below to evaluate result of actual_output and ideal_output
    methods.  Returns the ratio norm(ao-io)/norm(io) where norm is L2.
    Should assert ao and io are same size before calling this function.
    """
    prediction_error = ao.data - io.data
    enrm = np.linalg.norm(prediction_error)
    ionrm = np.linalg.norm(io.data)
    print("actual_output norm=",np.linalg.norm(ao.data))
    print("ideal_output norm=",ionrm)
    return enrm/ionrm

def test_RFdeconProcessor():
    """
    Test program for RFdeconProcessor class.  Duplicates some
    testing of RFdecon which uses this class.
    """
    # needed to find pf file in engine constructor
    os.environ["PFPATH"]="./data/pf"
    # Run the same sequence for all algorithms defined for
    # RFdeconProcessor
    alglist=["MultiTaperXcor","MultiTaperSpecDiv","LeastSquares","WaterLevel"]
    decon_processor = RFdeconProcessor(alg="MultiTaperXcor")

    seis_data0 = get_live_seismogram()
    seis_wavelet0 = get_live_seismogram()
    seis_noise0 = get_live_seismogram()

    for alg in alglist:
        print("testing algorithm=",alg)
        seis_data = Seismogram(seis_data0)
        seis_wavelet = Seismogram(seis_wavelet0)
        seis_noise = Seismogram(seis_noise0)
        processor = RFdeconProcessor(alg=alg)
        processor.loaddata(seis_data)
        processor.loadwavelet(seis_wavelet)
        if processor.uses_noise:
            processor.loadnoise(seis_noise)
        # first verify pickle work correctly
        processor_cpy = pickle.loads(pickle.dumps(processor))
        data = pickle.dumps(processor)
        processor_cpy = pickle.loads(data)

        assert (processor.dvector == processor_cpy.dvector).all()
        assert (processor.wvector == processor_cpy.wvector).all()
        if processor.uses_noise:
            assert (processor.nvector == processor_cpy.nvector).all()
        for k in processor.md.keys():
            assert processor.md[k] == processor_cpy.md[k]
        result = processor.apply()
        # in this test the output is meaningless - just verify length
        #assert len(result) == 1024
        ao = processor.actual_output()
        io = processor.ideal_output()
        prederr = prediction_error_norm(ao,io)
        print("prederr=",prederr)
        assert prederr < 0.1

    # this must be cleared to keep later pytest scripts from failing
    os.environ.pop('PFPATH', None)




def test_RFdecon():
    """
    Test program for RFdecon function.

    This function tests that RFdecon behaves correctly in the normal mode
    of operation where the input is a waveform segment that contains
    all the data required for the RF forms of decon.   That means we
    extract signal and noise from components.  Some algorithms also
    need to extract data from a preevent noise window.  That is tested
    independently here.
    """
    # needed to find pf file in engine constructor
    os.environ["PFPATH"]="./data/pf"
    # note this definition of 3000 samples at 20 sps and setting t0 to
    # -35 s must be consistent with data window parameters in the
    # RFdeconProcessor.pf file stored data/pf.
    seis0 = get_live_seismogram(3000, 20.0)
    seis0.t0 = -35.0
    # this is a list of algorithms supported by the RFdecon function
    # They can be enabled by a parameter on the function or by
    # passing an instance of the engine.  Ww test both below
    alglist=["LeastSquares","WaterLevel","MultiTaperXcor","MultiTaperSpecDiv"]
    # first test case with where the operator is instantiated on each call
    # to RFdecon
    for alg in alglist:
        d = Seismogram(seis0)
        # first verify it works without returnin actual and ideal wavelets
        d_decon = RFdecon(d,alg=alg)
        assert d_decon.live
        print(alg,d_decon.npts)

    # repeat the same loop as above but pass the engine as an arguments
    # Main difference here is we try before and after running the engine
    # through pickle.  That simulates how dask/spark would handle this
    # if RFdecon is used in a map operator
    for alg in alglist:
        print("Testing RFdecon with alg=",alg)
        d = Seismogram(seis0)
        deconengine = RFdeconProcessor(alg=alg)
        d_decon = RFdecon(d,alg=alg,engine=deconengine)
        assert d_decon.live
        print_metadata(d_decon)
        d = Seismogram(seis0)
        engine2 = pickle.loads(pickle.dumps(deconengine))
        #d_decon2 = RFdecon(d,alg=alg,engine=engine2)
        d_decon2 = RFdecon(d,alg=alg,engine=deconengine)
        assert d_decon2.live
        assert np.isclose(d_decon.data,d_decon2.data).all()
    # test variant of passing prewindowed data instead of v
    for alg in alglist:
        deconengine = RFdeconProcessor(alg=alg)
        d = Seismogram(seis0)
        n = WindowData(d,-30.0,-5.0)
        n = ExtractComponent(n,2)
        w = WindowData(d,deconengine.dwin.start,deconengine.dwin.end)
        w = ExtractComponent(w,2)
        d_decon = RFdecon(d,alg=alg,engine=deconengine,noisedata=n.data,wavelet=w.data)
        assert d_decon.live
    # this must be cleared to keep later pytest scripts from failing
    os.environ.pop('PFPATH', None)

def test_RFdecon_error_handlers():
    """
    This test function tests error handlers for RFdecon function.
    Key things to test are not dependent upon the algorithm so we don't
    need a loop of algorithm names like the tests above.   Most errors
    just kill the return but the content can be variable.  All the possiblel
    variations in what would be posted to elog are too difficult to cover
    so we only test generic handling as kills.
    """
    # needed to find pf file in engine constructor
    os.environ["PFPATH"]="./data/pf"
    # this starting point will work with defaults. We dither copies of it
    # to test error handling of common issues
    seis0 = get_live_seismogram(3000, 20.0)
    seis0.t0 = -35.0
    # which algorithm we used doesn't matter here - use defaults
    engine = RFdeconProcessor()
    # first a simple type test of arg0
    with pytest.raises(TypeError,match="only accepts mspass object"):
        foo = RFdecon("badarg0")
    # test handling dead datum
    d = Seismogram(seis0)
    d.kill()
    dret = RFdecon(d)
    assert dret.dead()
    # loose test for returning copy
    assert dret.t0 == d.t0
    assert dret.npts == d.npts
    # test error in data window
    # done here by dithering t0
    d = Seismogram(seis0)
    d.t0 += 1000.0
    dret = RFdecon(d)
    assert dret.dead()
    assert dret.elog.size()>0
    # slight variant with invalid noise window but valid data window
    # requires an algorithm that needs a noise window so use MultiTaperXcorDecon
    d = Seismogram(seis0)
    # assumes default data window start is -5
    d.t0 = -1.0
    dret = RFdecon(d,alg="MultiTaperXcor")
    assert dret.dead()
    assert dret.elog.size()>0
    # minor variant passing engine
    engine = RFdeconProcessor(alg="MultiTaperXcor")
    dret = RFdecon(d,engine=engine)
    assert dret.dead()
    assert dret.elog.size()>0
    # this must be cleared to keep later pytest scripts from failing
    os.environ.pop('PFPATH', None)
