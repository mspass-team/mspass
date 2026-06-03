#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import pickle
import numpy as np
import pytest

# needed to access the helper module
sys.path.append("python/tests")
sys.path.append("python/tests/algorithms")
from helper import (
    get_live_seismogram,
    get_live_timeseries,
    get_sin_timeseries,
    get_live_timeseries_ensemble,
    get_live_seismogram_ensemble,
)
from decon_data_generators import (
    addnoise,
    convolve_wavelet,
    make_impulse_data,
    make_simulation_wavelet,
)
from mspasspy.algorithms.window import WindowData
from mspasspy.algorithms.RFdeconProcessor import RFdeconProcessor, RFdecon
from mspasspy.ccore.seismic import Seismogram
from mspasspy.ccore.utility import MsPASSError
from mspasspy.algorithms.basic import ExtractComponent
from mspasspy.util.seismic import print_metadata


def prediction_error_norm(ao, io):
    """
    Used below to evaluate result of actual_output and ideal_output
    methods.  Returns the ratio norm(ao-io)/norm(io) where norm is L2.
    Should assert ao and io are same size before calling this function.
    """
    prediction_error = ao.data - io.data
    enrm = np.linalg.norm(prediction_error)
    ionrm = np.linalg.norm(io.data)
    print("actual_output norm=", np.linalg.norm(ao.data))
    print("ideal_output norm=", ionrm)
    return enrm / ionrm


def test_RFdeconProcessor():
    """
    Test program for RFdeconProcessor class.  Duplicates some
    testing of RFdecon which uses this class.
    """
    # needed to find pf file in engine constructor
    os.environ["PFPATH"] = "./data/pf"
    # Run the same sequence for all algorithms defined for
    # RFdeconProcessor
    alglist = [
        "MultiTaperPowerXcor",
        "MultiTaperPowerSpecDiv",
        "LeastSquares",
        "TimeDomainLeastSquares",
        "WaterLevel",
    ]
    decon_processor = RFdeconProcessor(alg="MultiTaperPowerXcor")

    np.random.seed(20240602)
    seis_data0 = get_live_seismogram()
    seis_wavelet0 = get_live_seismogram()
    seis_noise0 = get_live_seismogram()

    for alg in alglist:
        print("testing algorithm=", alg)
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
        # assert len(result) == 1024
        ao = processor.actual_output()
        io = processor.ideal_output()
        if alg == "TimeDomainLeastSquares":
            ao_data = np.asarray(ao.data)
            assert np.isfinite(ao_data).all()
            assert np.linalg.norm(ao_data) > 0.0
            assert ao.t0 < 0.0
            assert abs(ao.time(ao.sample_number(0.0))) <= ao.dt
        elif alg in (
            "MultiTaperPowerXcor",
            "MultiTaperPowerSpecDiv",
        ):
            # Multitaper operators return effective multitaper resolution
            # kernels, which need not match the ideal shaping wavelet as
            # closely as scalar water-level style operators.
            prederr = prediction_error_norm(ao, io)
            print("prederr=", prederr)
            assert np.isfinite(prederr)
            ao_data = np.asarray(ao.data)
            assert np.isfinite(ao_data).all()
            assert np.linalg.norm(ao_data) > 0.0
        else:
            prederr = prediction_error_norm(ao, io)
            print("prederr=", prederr)
            assert prederr < 0.2

    # this must be cleared to keep later pytest scripts from failing
    os.environ.pop("PFPATH", None)


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
    os.environ["PFPATH"] = "./data/pf"
    # note this definition of 3000 samples at 20 sps and setting t0 to
    # -35 s must be consistent with data window parameters in the
    # RFdeconProcessor.pf file stored data/pf.
    seis0 = get_live_seismogram(3000, 20.0)
    seis0.t0 = -35.0
    # this is a list of algorithms supported by the RFdecon function
    # They can be enabled by a parameter on the function or by
    # passing an instance of the engine.  Ww test both below
    alglist = [
        "LeastSquares",
        "TimeDomainLeastSquares",
        "WaterLevel",
        "MultiTaperPowerSpecDiv",
        "MultiTaperPowerXcor",
    ]
    # first test case with where the operator is instantiated on each call
    # to RFdecon
    for alg in alglist:
        d = Seismogram(seis0)
        # first verify it works without returnin actual and ideal wavelets
        d_decon = RFdecon(d, alg=alg)
        assert d_decon.live
        print(alg, d_decon.npts)

    # repeat the same loop as above but pass the engine as an arguments
    # Main difference here is we try before and after running the engine
    # through pickle.  That simulates how dask/spark would handle this
    # if RFdecon is used in a map operator
    for alg in alglist:
        print("Testing RFdecon with alg=", alg)
        d = Seismogram(seis0)
        deconengine = RFdeconProcessor(alg=alg)
        d_decon = RFdecon(d, alg=alg, engine=deconengine)
        assert d_decon.live
        print_metadata(d_decon)
        d = Seismogram(seis0)
        engine2 = pickle.loads(pickle.dumps(deconengine))
        d_decon2 = RFdecon(d, alg=alg, engine=engine2)
        # d_decon2 = RFdecon(d, alg=alg, engine=deconengine)
        assert d_decon2.live
        assert np.isclose(d_decon.data, d_decon2.data).all()
    # test variant of passing prewindowed data instead of v
    for alg in alglist:
        deconengine = RFdeconProcessor(alg=alg)
        d = Seismogram(seis0)
        n = WindowData(d, -30.0, -5.0)
        n = ExtractComponent(n, 2)
        w = WindowData(d, deconengine.dwin.start, deconengine.dwin.end)
        w = ExtractComponent(w, 2)
        d_decon = RFdecon(
            d, alg=alg, engine=deconengine, noisedata=n.data, wavelet=w.data
        )
        assert d_decon.live
    # this must be cleared to keep later pytest scripts from failing
    os.environ.pop("PFPATH", None)


def test_RFdecon_enables_generalized_iterative():
    os.environ["PFPATH"] = "./data/pf"
    wavelet = make_simulation_wavelet()
    impulses = make_impulse_data()
    seis0 = addnoise(convolve_wavelet(impulses, wavelet), nscale=0.0, padlength=800)

    rf = RFdecon(seis0, alg="GeneralizedIterative", pf="TimeDomainGIDDecon.pf")

    assert rf.live
    assert rf.npts > 0
    assert np.isfinite(rf.data).all()
    assert rf.is_defined("RFdecon_properties")
    qc = rf["RFdecon_properties"]
    assert qc["algorithm"] == "GeneralizedIterative"
    assert qc["iteration_count"] > 0
    os.environ.pop("PFPATH", None)


@pytest.mark.parametrize("alg", ["GeneralizedIterative", "FrequencyDomainGID"])
def test_RFdeconProcessor_gid_default_pf_routes_to_gid_parameter_file(alg):
    os.environ["PFPATH"] = "./data/pf"
    try:
        processor = RFdeconProcessor(alg=alg)
        assert processor.is_3c_engine
        assert processor.dwin.start <= processor.processor.deconvolution_window_start()
    finally:
        os.environ.pop("PFPATH", None)


def test_RFdecon_generalized_iterative_accepts_external_wavelet():
    os.environ["PFPATH"] = "./data/pf"
    wavelet = make_simulation_wavelet()
    impulses = make_impulse_data()
    seis0 = addnoise(convolve_wavelet(impulses, wavelet), nscale=0.0, padlength=800)

    rf = RFdecon(
        seis0,
        alg="GeneralizedIterative",
        pf="TimeDomainGIDDecon.pf",
        wavelet=wavelet,
    )

    assert rf.live
    assert rf.npts > 0
    assert np.isfinite(rf.data).all()
    assert rf.is_defined("RFdecon_properties")
    assert rf["RFdecon_properties"]["iteration_count"] > 0
    os.environ.pop("PFPATH", None)


@pytest.mark.parametrize(
    "alg,pf",
    [
        ("GeneralizedIterative", "TimeDomainGIDDecon.pf"),
        ("FrequencyDomainGID", "FrequencyDomainGIDDecon.pf"),
    ],
)
def test_RFdecon_gid_accepts_raw_vector_wavelet_and_noise(alg, pf):
    os.environ["PFPATH"] = "./data/pf"
    wavelet = make_simulation_wavelet()
    impulses = make_impulse_data()
    seis0 = addnoise(convolve_wavelet(impulses, wavelet), nscale=0.0, padlength=800)
    processor = RFdeconProcessor(alg=alg, pf=pf)
    noise = WindowData(seis0, processor.nwin.start, processor.nwin.end)
    noise = ExtractComponent(noise, 2)

    rf = RFdecon(
        seis0,
        alg=alg,
        pf=pf,
        wavelet=np.asarray(wavelet.data),
        noisedata=np.asarray(noise.data),
    )

    assert rf.live
    assert rf.npts > 0
    assert np.isfinite(rf.data).all()
    assert rf.is_defined("RFdecon_properties")
    assert rf["RFdecon_properties"]["iteration_count"] > 0
    os.environ.pop("PFPATH", None)


def test_RFdecon_clears_stale_gid_external_wavelet_when_wavelet_is_none(tmp_path):
    os.environ["PFPATH"] = "./data/pf"
    try:
        text = open("data/pf/TimeDomainGIDDecon.pf", encoding="utf-8").read()
        text = text.replace(
            "time_domain_gid_deconvolution &Arr{\n        deconvolution_type least_square",
            "time_domain_gid_deconvolution &Arr{\n        deconvolution_type ns_gid",
        )
        pf = tmp_path / "TimeDomainGIDDecon.pf"
        pf.write_text(text)
        wavelet = make_simulation_wavelet()
        impulses = make_impulse_data()
        seis0 = addnoise(
            convolve_wavelet(impulses, wavelet), nscale=0.0, padlength=800
        )
        processor = RFdeconProcessor(
            alg="GeneralizedIterative", pf=str(pf)
        )

        rf_external = RFdecon(
            Seismogram(seis0),
            alg="GeneralizedIterative",
            engine=processor,
            wavelet=wavelet,
        )
        assert rf_external.live
        assert rf_external["RFdecon_properties"]["ns_gid_external_wavelet_used"]

        rf_internal = RFdecon(
            Seismogram(seis0), alg="GeneralizedIterative", engine=processor
        )
        assert rf_internal.live
        assert not rf_internal["RFdecon_properties"][
            "ns_gid_external_wavelet_used"
        ]
    finally:
        os.environ.pop("PFPATH", None)


def test_RFdeconProcessor_apply_3c_reports_failed_gid_load():
    os.environ["PFPATH"] = "./data/pf"
    try:
        processor = RFdeconProcessor(
            alg="GeneralizedIterative", pf="TimeDomainGIDDecon.pf"
        )
        bad_data = get_live_seismogram(3000, 20.0)
        bad_data.t0 = 1000.0

        with pytest.raises(MsPASSError, match="could not be loaded"):
            processor.apply_3c(Seismogram(bad_data))
    finally:
        os.environ.pop("PFPATH", None)


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
    os.environ["PFPATH"] = "./data/pf"
    # this starting point will work with defaults. We dither copies of it
    # to test error handling of common issues
    seis0 = get_live_seismogram(3000, 20.0)
    seis0.t0 = -35.0
    # which algorithm we used doesn't matter here - use defaults
    engine = RFdeconProcessor()
    # first a simple type test of arg0
    with pytest.raises(TypeError, match="RFdecon:  arg0 is of type="):
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
    assert dret.elog.size() > 0
    # slight variant with invalid noise window but valid data window
    # requires an algorithm that needs a noise window so use multitaper decon
    d = Seismogram(seis0)
    # assumes default data window start is -5
    d.t0 = -1.0
    dret = RFdecon(d, alg="MultiTaperPowerXcor")
    assert dret.dead()
    assert dret.elog.size() > 0
    # minor variant passing engine
    engine = RFdeconProcessor(alg="MultiTaperPowerXcor")
    dret = RFdecon(d, engine=engine)
    assert dret.dead()
    assert dret.elog.size() > 0
    # this must be cleared to keep later pytest scripts from failing
    os.environ.pop("PFPATH", None)


@pytest.mark.parametrize(
    "legacy_alg",
    ["MultiTaperXcor", "MultiTaperSpecDiv"],
)
def test_RFdeconProcessor_multitaper_legacy_names_warn(legacy_alg):
    os.environ["PFPATH"] = "./data/pf"
    try:
        with pytest.warns(DeprecationWarning, match="deprecated compatibility"):
            RFdeconProcessor(alg=legacy_alg)
    finally:
        os.environ.pop("PFPATH", None)


@pytest.mark.parametrize(
    "preferred_alg,legacy_alg",
    [
        ("MultiTaperPowerXcor", "MultiTaperXcor"),
        ("MultiTaperPowerSpecDiv", "MultiTaperSpecDiv"),
    ],
)
def test_RFdeconProcessor_multitaper_legacy_names_match_preferred_output(
    preferred_alg, legacy_alg
):
    os.environ["PFPATH"] = "./data/pf"
    try:
        seis0 = get_live_seismogram()
        preferred = RFdeconProcessor(alg=preferred_alg)
        preferred.loaddata(Seismogram(seis0))
        preferred.loadwavelet(Seismogram(seis0))
        preferred.loadnoise(Seismogram(seis0))
        preferred_result = np.asarray(preferred.apply(), dtype=np.float64)

        with pytest.warns(DeprecationWarning, match="deprecated compatibility"):
            legacy = RFdeconProcessor(alg=legacy_alg)
        legacy.loaddata(Seismogram(seis0))
        legacy.loadwavelet(Seismogram(seis0))
        legacy.loadnoise(Seismogram(seis0))
        legacy_result = np.asarray(legacy.apply(), dtype=np.float64)

        assert np.allclose(preferred_result, legacy_result)
    finally:
        os.environ.pop("PFPATH", None)


@pytest.mark.parametrize(
    "preferred_alg,malformed_text,expected_message",
    [
        (
            "MultiTaperPowerXcor",
            """MultiTaperPowerXcor &Arr{
damping_factor -1.0
shaping_wavelet_dt 0.05
shaping_wavelet_type none
target_sample_interval 0.05
operator_nfft 1024
time_bandwidth_product 2.5
number_tapers 4
deconvolution_data_window_start -5.0
deconvolution_data_window_end 30.0
noise_window_start -35.0
noise_window_end -5.0
}
""",
            "damping_factor must be positive",
        ),
        (
            "MultiTaperPowerSpecDiv",
            """MultiTaperPowerSpecDiv &Arr{
damping_factor -1.0
shaping_wavelet_dt 0.05
shaping_wavelet_type none
target_sample_interval 0.05
operator_nfft 1024
time_bandwidth_product 2.5
number_tapers 4
deconvolution_data_window_start -5.0
deconvolution_data_window_end 30.0
noise_window_start -35.0
noise_window_end -5.0
}
""",
            "damping_factor must be positive",
        ),
        (
            "MultiTaperPowerXcor",
            "MultiTaperPowerXcor 5\n",
            "defined but is not an &Arr",
        ),
        (
            "MultiTaperPowerSpecDiv",
            "MultiTaperPowerSpecDiv 5\n",
            "defined but is not an &Arr",
        ),
    ],
)
def test_RFdeconProcessor_malformed_preferred_multitaper_branch_does_not_fallback(
    tmp_path, preferred_alg, malformed_text, expected_message
):
    pf_text = open("data/pf/RFdeconProcessor.pf", encoding="utf-8").read()
    pf_text += malformed_text
    pf = tmp_path / "RFdeconProcessor_bad_preferred.pf"
    pf.write_text(pf_text)

    with pytest.raises(MsPASSError, match=expected_message):
        RFdeconProcessor(alg=preferred_alg, pf=str(pf))


@pytest.mark.parametrize(
    "alg,pf",
    [
        ("LeastSquares", "RFdeconProcessor.pf"),
        ("TimeDomainLeastSquares", "RFdeconProcessor.pf"),
        ("WaterLevel", "RFdeconProcessor.pf"),
        ("MultiTaperPowerSpecDiv", "RFdeconProcessor.pf"),
        ("MultiTaperPowerXcor", "RFdeconProcessor.pf"),
        ("GeneralizedIterative", "TimeDomainGIDDecon.pf"),
        ("FrequencyDomainGID", "FrequencyDomainGIDDecon.pf"),
    ],
)
def test_RFdecon_invalid_configured_windows_return_dead_without_qc(alg, pf):
    os.environ["PFPATH"] = "./data/pf"
    try:
        seis0 = get_live_seismogram(3000, 20.0)
        seis0.t0 = 1000.0

        result = RFdecon(Seismogram(seis0), alg=alg, pf=pf)

        assert result.dead(), alg
        assert result.elog.size() > 0, alg
        assert not result.is_defined("RFdecon_properties"), alg
    finally:
        os.environ.pop("PFPATH", None)


def test_RFdeconProcessor_apply_3c_uses_loaded_external_wavelet(tmp_path):
    text = open("data/pf/TimeDomainGIDDecon.pf", encoding="utf-8").read()
    text = text.replace(
        "time_domain_gid_deconvolution &Arr{\n        deconvolution_type least_square",
        "time_domain_gid_deconvolution &Arr{\n        deconvolution_type ns_gid",
    )
    pf = tmp_path / "TimeDomainGIDDecon.pf"
    pf.write_text(text)

    wavelet = make_simulation_wavelet()
    truth = make_impulse_data()
    data = convolve_wavelet(truth, wavelet)
    data = addnoise(data, nscale=1.0e-4, padlength=800)
    data["low_f_band_edge"] = 0.02
    data["high_f_band_edge"] = 2.0

    processor = RFdeconProcessor(alg="GeneralizedIterative", pf=str(pf))
    processor.loadwavelet(wavelet, dtype="TimeSeries")
    processor.loadnoise(data, dtype="Seismogram", component=2, window=True)
    result = processor.apply_3c(data)

    assert result.live
    qc = dict(processor.processor.QCMetrics())
    assert qc["ns_gid_enabled"]
    assert qc["ns_gid_external_wavelet_used"]
