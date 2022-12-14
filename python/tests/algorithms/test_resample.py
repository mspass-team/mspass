import numpy as np
from helper import (
    get_live_seismogram,
    get_live_timeseries,
    get_sin_timeseries,
    get_live_timeseries_ensemble,
    get_live_seismogram_ensemble,
)
from mspasspy.ccore.seismic import (
    TimeSeries,
    Seismogram,
    TimeSeriesEnsemble,
    SeismogramEnsemble)
import matplotlib.pyplot as plt

from mspasspy.algorithms.resample import ObspyDecimator,ObspyResampler

def test_resample():
    ts = get_sin_timeseries(sampling_rate=100.0)
    # have to save this because ts is overwritten by resample method
    ts0 = TimeSeries(ts)
    ts_npts = ts.npts
    # upsample test for resampler
    upsampler=ObspyResampler(250.0)
    tsup = upsampler.resample(ts)
    assert np.isclose(tsup.dt,0.004)
    # This computed npts is more robust.  Otherwise changes in helper 
    # would break it
    npup = int(ts_npts*250.0/100.0)
    assert tsup.npts==npup   # weird number = int(255*250/100)
    # now repeat for downsampling with resample algorithm
    ts=TimeSeries(ts0)
    ds_resampler = ObspyResampler(5.0)
    tsds = ds_resampler.resample(ts)
    # Note plots of this output show the auto antialiasing works as 
    # advertised in scipy
    assert np.isclose(tsds.dt,0.2)
    assert tsds.npts == int(ts_npts*5.0/100.0)
    # Repeat same downsampling with decimate
    ts=TimeSeries(ts0)
    decimator = ObspyDecimator(5.0)
    tsds = decimator.resample(ts)
    assert np.isclose(tsds.dt,0.2)
    # the documentation doesn't tell me why by the scipy decimate 
    # function seems to round npts up rather than use int
    assert tsds.npts == int(ts_npts*5.0/100.0)+1
    seis = get_live_seismogram()
    seis0=Seismogram(seis)
    seis = upsampler.resample(seis)
    assert np.isclose(seis.dt,0.004)
    npup = int(seis0.npts*250.0/20.0)
    assert seis.npts == npup
    seis=Seismogram(seis0)
    seis = ds_resampler.resample(seis)
    assert np.isclose(seis.dt,0.2)
    assert seis.npts == int(ts_npts*5.0/20.0)
    seis=Seismogram(seis0)
    seis = decimator.resample(seis)
    # again the round issue noted above
    dec_npts = int(seis0.npts*5.0/20.0) + 1 
    assert np.isclose(seis.dt,0.2)
    assert seis.npts == dec_npts
    
    tse = get_live_timeseries_ensemble(5)
    tse.set_live()
    tse0 = TimeSeriesEnsemble(tse)
    tse = upsampler.resample(tse)
    npup = int(tse0.member[0].npts*250.0/20.0)
    for d in tse.member:
        assert d.live
        assert np.isclose(d.dt,0.004)
        assert d.npts == npup
        
    tse = TimeSeriesEnsemble(tse0)
    tse = ds_resampler.resample(tse)
    npup = int(tse0.member[0].npts*5.0/20.0)
    for d in tse.member:
        assert d.live
        assert np.isclose(d.dt,0.2)
        assert d.npts == npup
    
    tse = TimeSeriesEnsemble(tse0)
    tse = decimator.resample(tse)
    npup = int(tse0.member[0].npts*5.0/20.0)+1
    for d in tse.member:
        assert d.live
        assert np.isclose(d.dt,0.2)
        assert d.npts == npup
    
    seis_e = get_live_seismogram_ensemble(3)
    seis_e0 = SeismogramEnsemble(seis_e)
    seis_e = upsampler.resample(seis_e)
    npup = int(seis_e0.member[0].npts*250.0/20.0)
    for d in seis_e.member:
        assert d.live
        assert np.isclose(d.dt,0.004)
        assert d.npts == npup
        
    seis_e = SeismogramEnsemble(seis_e0)
    seis_e = ds_resampler.resample(seis_e)
    npup = int(seis_e0.member[0].npts*5.0/20.0)
    for d in seis_e.member:
        assert d.live
        assert np.isclose(d.dt,0.2)
        assert d.npts == npup
    
    seis_e = SeismogramEnsemble(seis_e0)
    seis_e = decimator.resample(seis_e)
    npup = int(seis_e0.member[0].npts*5.0/20.0)+1
    for d in seis_e.member:
        assert d.live
        assert np.isclose(d.dt,0.2)
        assert d.npts == npup
  
