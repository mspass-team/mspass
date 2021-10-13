#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import pickle
import numpy as np
import pytest

sys.path.append("python/tests")
from helper import (get_live_seismogram,
                    get_live_timeseries,
                    get_sin_timeseries,
                    get_live_timeseries_ensemble,
                    get_live_seismogram_ensemble)
from mspasspy.algorithms.window import WindowData
from mspasspy.algorithms.RFdeconProcessor import RFdeconProcessor, RFdecon

def test_RFdeconProcessor():
    decon_processor = RFdeconProcessor(alg="MultiTaperXcor")

    seis_data = get_live_seismogram()
    seis_wavelet = get_live_seismogram()
    seis_noise = get_live_seismogram()

    decon_processor.loaddata(seis_data)
    decon_processor.loadwavelet(seis_wavelet)
    decon_processor.loadnoise(seis_noise)

    # decon_processor_copy = pickle.loads(pickle.dumps(decon_processor))
    data = pickle.dumps(decon_processor)
    decon_processor_copy = pickle.loads(data)

    assert (decon_processor.dvector == decon_processor_copy.dvector).all()
    assert (decon_processor.wvector == decon_processor_copy.wvector).all()
    assert (decon_processor.nvector == decon_processor_copy.nvector).all()

    seis_data.npts = 8
    seis_data.data[2] = [1,-1,0,0,0,0,0,0]
    seis_data.data[1] = [0,1,-1,0,0,0,0,0]
    seis_data.data[0] = [0,0,-1,1,0,0,0,0]

    decon_processor = RFdeconProcessor()
    decon_processor.loaddata(seis_data)
    decon_processor.loadwavelet(seis_data)
    decon_processor_copy = pickle.loads(pickle.dumps(decon_processor))
    result1 = np.array(decon_processor.apply())
    result2 = np.array(decon_processor_copy.apply())
    assert all(abs(a-b) < 1e-6 for a,b in zip(result1, result2))

def test_RFdeconr():
    seis1 = get_live_seismogram(71, 2.0)
    seis1.t0 = -5

    seis2 = get_live_seismogram(71, 2.0)
    seis2.t0 = -5
    for i in range(3):
        for j in range(seis2.npts):
            seis2.data[i, j] = seis1.data[i, j]

    processor = RFdeconProcessor()
    processor.loaddata(seis1)
    processor.loadnoise(seis1, window=True)
    processor.loadwavelet(seis1, window=True)
    result1 = WindowData(seis1, processor.dwin.start, processor.dwin.end)
    for k in range(3):
        processor.loaddata(result1, component=k)
        x = processor.apply()
        for i in range(seis1.npts):
            result1.data[k, i] = x[i]

    result2 = RFdecon(seis2)

    for k in range(3):
        assert all(abs(a-b) < 1e-6 for a,b in zip(result1.data[k], result2.data[k]))