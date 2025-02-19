import sys

sys.path.append("python/tests")
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
    assert np.isclose(tsup["sampling_rate"], 250.0)
    # This computed npts is more robust.  Otherwise changes in helper
    # would break it
    npup = int(ts_npts * 250.0 / 100.0)
    assert tsup.npts == npup  # weird number = int(255*250/100)

    # test for the case when sampling_rate is not defined
    ts = get_sin_timeseries(sampling_rate=100.0)
    # have to save this because ts is overwritten by resample method
    ts0 = TimeSeries(ts)
    ts_npts = ts.npts
    # remove sampling_rate
    del ts["sampling_rate"]
    assert not ts.is_defined("sampling_rate")
    # upsample test for resampler
    upsampler = ScipyResampler(250.0)

    tsup = upsampler.resample(ts)
    assert np.isclose(tsup.dt, 0.004)
    assert ts.is_defined("sampling_rate")

    # now repeat for downsampling with resample algorithm
    ts = TimeSeries(ts0)
    ds_resampler = ScipyResampler(5.0)
    assert ts.is_defined("sampling_rate")
    tsds = ds_resampler.resample(ts)
    # Note plots of this output show the auto antialiasing works as
    # advertised in scipy
    assert np.isclose(tsds.dt, 0.2)
    assert np.isclose(tsds["sampling_rate"], 5.0)
    assert tsds.npts == int(ts_npts * 5.0 / 100.0)
    # Repeat same downsampling with decimate
    ts = TimeSeries(ts0)
    decimator = ScipyDecimator(5.0)
    assert ts.is_defined("sampling_rate")
    tsds = decimator.resample(ts)
    assert np.isclose(tsds.dt, 0.2)
    assert np.isclose(tsds["sampling_rate"], 5.0)
    # the documentation doesn't tell me why by the scipy decimate
    # function seems to round npts up rather than use int
    assert tsds.npts == int(ts_npts * 5.0 / 100.0) + 1
    seis = get_live_seismogram()
    seis0 = Seismogram(seis)
    assert seis.is_defined("sampling_rate")
    seis = upsampler.resample(seis)
    assert np.isclose(seis.dt, 0.004)
    assert np.isclose(seis["sampling_rate"], 250.0)
    npup = int(seis0.npts * 250.0 / 20.0)
    assert seis.npts == npup
    seis = Seismogram(seis0)
    assert seis.is_defined("sampling_rate")
    seis = ds_resampler.resample(seis)
    assert np.isclose(seis.dt, 0.2)
    assert np.isclose(seis["sampling_rate"], 5.0)
    assert seis.npts == int(ts_npts * 5.0 / 20.0)
    seis = Seismogram(seis0)
    assert seis.is_defined("sampling_rate")
    seis = decimator.resample(seis)
    # again the round issue noted above
    dec_npts = int(seis0.npts * 5.0 / 20.0) + 1
    assert np.isclose(seis.dt, 0.2)
    assert np.isclose(seis["sampling_rate"], 5.0)
    assert seis.npts == dec_npts

    tse = get_live_timeseries_ensemble(5)
    tse.set_live()
    tse0 = TimeSeriesEnsemble(tse)
    assert tse.member[0].is_defined("sampling_rate")
    tse = upsampler.resample(tse)
    npup = int(tse0.member[0].npts * 250.0 / 20.0)
    for d in tse.member:
        assert d.live
        assert np.isclose(d.dt, 0.004)
        assert np.isclose(d["sampling_rate"], 250.0)
        assert d.npts == npup

    tse = TimeSeriesEnsemble(tse0)
    assert tse.member[0].is_defined("sampling_rate")
    tse = ds_resampler.resample(tse)
    npup = int(tse0.member[0].npts * 5.0 / 20.0)
    for d in tse.member:
        assert d.live
        assert np.isclose(d.dt, 0.2)
        assert np.isclose(d["sampling_rate"], 5.0)
        assert d.npts == npup

    tse = TimeSeriesEnsemble(tse0)
    tse = decimator.resample(tse)
    npup = int(tse0.member[0].npts * 5.0 / 20.0) + 1
    for d in tse.member:
        assert d.live
        assert np.isclose(d.dt, 0.2)
        assert np.isclose(d["sampling_rate"], 5.0)
        assert d.npts == npup

    seis_e = get_live_seismogram_ensemble(3)
    seis_e0 = SeismogramEnsemble(seis_e)
    assert seis_e.member[0].is_defined("sampling_rate")
    seis_e = upsampler.resample(seis_e)
    npup = int(seis_e0.member[0].npts * 250.0 / 20.0)
    for d in seis_e.member:
        assert d.live
        assert np.isclose(d.dt, 0.004)
        assert np.isclose(d["sampling_rate"], 250.0)
        assert d.npts == npup

    seis_e = SeismogramEnsemble(seis_e0)
    assert seis_e.member[0].is_defined("sampling_rate")
    seis_e = ds_resampler.resample(seis_e)
    npup = int(seis_e0.member[0].npts * 5.0 / 20.0)
    for d in seis_e.member:
        assert d.live
        assert np.isclose(d.dt, 0.2)
        assert np.isclose(d["sampling_rate"], 5.0)
        assert d.npts == npup

    seis_e = SeismogramEnsemble(seis_e0)
    assert seis_e.member[0].is_defined("sampling_rate")
    seis_e = decimator.resample(seis_e)
    npup = int(seis_e0.member[0].npts * 5.0 / 20.0) + 1
    for d in seis_e.member:
        assert d.live
        assert np.isclose(d.dt, 0.2)
        assert np.isclose(d["sampling_rate"], 5.0)
        assert d.npts == npup
    # Now test resample function.   We define two operators
    # for 40 sps target
    resample40 = ScipyResampler(40.0)
    decimate40 = ScipyDecimator(40.0)
    assert ts.is_defined("sampling_rate")
    d = resample(ts, decimate40, resample40)
    assert d.dt == 0.025
    assert d["sampling_rate"] == 40.0
    assert d.live
    assert d.npts == 104
    # print(d.dt,d.live,d.npts)
    assert ts0.is_defined("sampling_rate")
    d = resample(ts0, decimate40, resample40)
    # print(d.dt,d.live,d.npts)
    assert d.dt == 0.025
    assert d["sampling_rate"] == 40.0
    assert d.live
    assert d.npts == 101
    assert tse0.member[0].is_defined("sampling_rate")
    d = resample(tse0, decimate40, resample40)
    # print("tse0")
    for d in tse0.member:
        # print(d.dt,d.live,d.npts)
        assert d.dt == 0.025
        assert d["sampling_rate"] == 40.0
        assert d.live
        assert d.npts == 510
    assert tse.member[0].is_defined("sampling_rate")
    d = resample(tse, decimate40, resample40)
    # print('tse')
    for d in tse0.member:
        # print(d.dt,d.live,d.npts)
        assert d.dt == 0.025
        assert d["sampling_rate"] == 40.0
        assert d.live
        assert d.npts == 510

    assert seis.is_defined("sampling_rate")
    d = resample(seis, decimate40, resample40)
    # print(d.dt,d.live,d.npts)
    assert seis0.is_defined("sampling_rate")
    d = resample(seis0, decimate40, resample40)
    # print(d.dt,d.live,d.npts)
    assert seis_e.member[0].is_defined("sampling_rate")
    d = resample(seis_e, decimate40, resample40)
    # print('seis_e')
    for d in seis_e.member:
        # print(d.dt,d.live,d.npts)
        assert d.dt == 0.025
        assert d["sampling_rate"] == 40.0
        assert d.live
        assert d.npts == 512
    assert seis_e0.member[0].is_defined("sampling_rate")
    d = resample(seis_e0, decimate40, resample40)
    # print('seis_e0')
    for d in seis_e0.member:
        # print(d.dt,d.live,d.npts)
        assert d.dt == 0.025
        assert d["sampling_rate"] == 40.0
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
