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
    SeismogramEnsemble,
)
from mspasspy.ccore.utility import MsPASSError

from mspasspy.algorithms.resample import (
    ScipyDecimator,
    ScipyResampler,
    resample,
)


def test_resample():
    ts = get_sin_timeseries(sampling_rate=100.0)
    # have to save this because ts is overwritten by resample method
    ts0 = TimeSeries(ts)
    ts_npts = ts.npts
    # upsample test for resampler
    upsampler = ScipyResampler(250.0)
    tsup = upsampler.resample(ts)
    assert np.isclose(tsup.dt, 0.004)
    # This computed npts is more robust.  Otherwise changes in helper
    # would break it
    npup = int(ts_npts * 250.0 / 100.0)
    assert tsup.npts == npup  # weird number = int(255*250/100)
    # now repeat for downsampling with resample algorithm
    ts = TimeSeries(ts0)
    ds_resampler = ScipyResampler(5.0)
    tsds = ds_resampler.resample(ts)
    # Note plots of this output show the auto antialiasing works as
    # advertised in scipy
    assert np.isclose(tsds.dt, 0.2)
    assert tsds.npts == int(ts_npts * 5.0 / 100.0)
    # Repeat same downsampling with decimate
    ts = TimeSeries(ts0)
    decimator = ScipyDecimator(5.0)
    tsds = decimator.resample(ts)
    assert np.isclose(tsds.dt, 0.2)
    # the documentation doesn't tell me why by the scipy decimate
    # function seems to round npts up rather than use int
    assert tsds.npts == int(ts_npts * 5.0 / 100.0) + 1
    seis = get_live_seismogram()
    seis0 = Seismogram(seis)
    seis = upsampler.resample(seis)
    assert np.isclose(seis.dt, 0.004)
    npup = int(seis0.npts * 250.0 / 20.0)
    assert seis.npts == npup
    seis = Seismogram(seis0)
    seis = ds_resampler.resample(seis)
    assert np.isclose(seis.dt, 0.2)
    assert seis.npts == int(ts_npts * 5.0 / 20.0)
    seis = Seismogram(seis0)
    seis = decimator.resample(seis)
    # again the round issue noted above
    dec_npts = int(seis0.npts * 5.0 / 20.0) + 1
    assert np.isclose(seis.dt, 0.2)
    assert seis.npts == dec_npts

    tse = get_live_timeseries_ensemble(5)
    tse.set_live()
    tse0 = TimeSeriesEnsemble(tse)
    tse = upsampler.resample(tse)
    npup = int(tse0.member[0].npts * 250.0 / 20.0)
    for d in tse.member:
        assert d.live
        assert np.isclose(d.dt, 0.004)
        assert d.npts == npup

    tse = TimeSeriesEnsemble(tse0)
    tse = ds_resampler.resample(tse)
    npup = int(tse0.member[0].npts * 5.0 / 20.0)
    for d in tse.member:
        assert d.live
        assert np.isclose(d.dt, 0.2)
        assert d.npts == npup

    tse = TimeSeriesEnsemble(tse0)
    tse = decimator.resample(tse)
    npup = int(tse0.member[0].npts * 5.0 / 20.0) + 1
    for d in tse.member:
        assert d.live
        assert np.isclose(d.dt, 0.2)
        assert d.npts == npup

    seis_e = get_live_seismogram_ensemble(3)
    seis_e0 = SeismogramEnsemble(seis_e)
    seis_e = upsampler.resample(seis_e)
    npup = int(seis_e0.member[0].npts * 250.0 / 20.0)
    for d in seis_e.member:
        assert d.live
        assert np.isclose(d.dt, 0.004)
        assert d.npts == npup

    seis_e = SeismogramEnsemble(seis_e0)
    seis_e = ds_resampler.resample(seis_e)
    npup = int(seis_e0.member[0].npts * 5.0 / 20.0)
    for d in seis_e.member:
        assert d.live
        assert np.isclose(d.dt, 0.2)
        assert d.npts == npup

    seis_e = SeismogramEnsemble(seis_e0)
    seis_e = decimator.resample(seis_e)
    npup = int(seis_e0.member[0].npts * 5.0 / 20.0) + 1
    for d in seis_e.member:
        assert d.live
        assert np.isclose(d.dt, 0.2)
        assert d.npts == npup
    # Now test resample function.   We define two operators
    # for 40 sps target
    resample40 = ScipyResampler(40.0)
    decimate40 = ScipyDecimator(40.0)

    d = resample(ts, decimate40, resample40)
    assert d.dt == 0.025
    assert d.live
    assert d.npts == 104
    # print(d.dt,d.live,d.npts)
    d = resample(ts0, decimate40, resample40)
    # print(d.dt,d.live,d.npts)
    assert d.dt == 0.025
    assert d.live
    assert d.npts == 101
    d = resample(tse0, decimate40, resample40)
    # print("tse0")
    for d in tse0.member:
        # print(d.dt,d.live,d.npts)
        assert d.dt == 0.025
        assert d.live
        assert d.npts == 510
    d = resample(tse, decimate40, resample40)
    # print('tse')
    for d in tse0.member:
        # print(d.dt,d.live,d.npts)
        assert d.dt == 0.025
        assert d.live
        assert d.npts == 510

    d = resample(seis, decimate40, resample40)
    # print(d.dt,d.live,d.npts)
    d = resample(seis0, decimate40, resample40)
    # print(d.dt,d.live,d.npts)
    d = resample(seis_e, decimate40, resample40)
    # print('seis_e')
    for d in seis_e.member:
        # print(d.dt,d.live,d.npts)
        assert d.dt == 0.025
        assert d.live
        assert d.npts == 512
    d = resample(seis_e0, decimate40, resample40)
    # print('seis_e0')
    for d in seis_e0.member:
        # print(d.dt,d.live,d.npts)
        assert d.dt == 0.025
        assert d.live
        assert d.npts == 510

    # Test verify mode mode sample interval mismatch exception
    try:
        d = resample(tse, decimator, resample40)
        print("Error - should not get here")
        assert False
    except MsPASSError as merr:
        print("Handled exception properly")
    else:
        print("Error - should not be here")
        assert False
