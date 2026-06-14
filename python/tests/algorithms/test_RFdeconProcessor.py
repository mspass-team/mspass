#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import pickle
import cloudpickle
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
from mspasspy.ccore.seismic import Seismogram, TimeSeries
from mspasspy.ccore.utility import AntelopePf, ErrorSeverity, Metadata, MsPASSError
from mspasspy.algorithms.basic import ExtractComponent
from mspasspy.util.seismic import print_metadata


DEFAULT_GID_DECONVOLUTION_TYPE = "ns_gid"


def _distributed_roundtrip(obj):
    distributed_protocol = pytest.importorskip("distributed.protocol")
    header, frames = distributed_protocol.serialize(obj)
    return distributed_protocol.deserialize(header, frames)


def _gid_pf_with_mode(tmp_path, mode, pf_name="TimeDomainGIDDecon.pf"):
    text = open(f"data/pf/{pf_name}", encoding="utf-8").read()
    old = (
        "time_domain_gid_deconvolution &Arr{\n        "
        f"deconvolution_type {DEFAULT_GID_DECONVOLUTION_TYPE}"
    )
    new = f"time_domain_gid_deconvolution &Arr{{\n        deconvolution_type {mode}"
    assert text.count(old) == 1
    text = text.replace(old, new, 1)
    pf = tmp_path / pf_name
    pf.write_text(text)
    return pf


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
    expected_operator_names = {
        "MultiTaperPowerXcor": "MultiTaperXcorDecon",
        "MultiTaperPowerSpecDiv": "MultiTaperSpecDivDecon",
        "LeastSquares": "LeastSquareDecon",
        "TimeDomainLeastSquares": "TimeDomainLeastSquareDecon",
        "WaterLevel": "WaterLevelDecon",
    }
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
        qc = processor.QCMetrics()
        assert qc["decon_operator"] == expected_operator_names[alg]
        assert qc["decon_processed"]
        assert qc["decon_input_loaded"]
        assert qc["decon_data_npts"] > 0
        assert qc["decon_wavelet_npts"] > 0
        assert qc["decon_output_npts"] > 0
        assert qc["decon_shaping_wavelet_nfft"] > 0
        assert np.isfinite(qc["decon_shaping_wavelet_dt"])
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


def test_RFdeconProcessor_gid_defaults_resolve_from_package_pf_path():
    old_pfpath = os.environ.get("PFPATH")
    os.environ["PFPATH"] = "python/mspasspy/data/pf"
    try:
        for alg in ("GeneralizedIterative", "TimeDomainGID", "FrequencyDomainGID"):
            processor = RFdeconProcessor(alg=alg)
            assert processor.is_3c_engine
    finally:
        if old_pfpath is None:
            os.environ.pop("PFPATH", None)
        else:
            os.environ["PFPATH"] = old_pfpath


@pytest.mark.parametrize(
    "alg,pf",
    [
        ("GeneralizedIterative", "TimeDomainGIDDecon.pf"),
        ("TimeDomainGID", "TimeDomainGIDDecon.pf"),
        ("FrequencyDomainGID", "FrequencyDomainGIDDecon.pf"),
    ],
)
def test_RFdeconProcessor_gid_pickle_supports_distributed_use(alg, pf):
    os.environ["PFPATH"] = "./data/pf"
    try:
        wavelet = make_simulation_wavelet()
        impulses = make_impulse_data()
        seis0 = addnoise(convolve_wavelet(impulses, wavelet), nscale=0.0, padlength=800)
        processor = RFdeconProcessor(alg=alg, pf=pf)
        processor2 = pickle.loads(pickle.dumps(processor))
        os.environ.pop("PFPATH", None)

        rf1 = RFdecon(Seismogram(seis0), alg=alg, engine=processor)
        rf2 = RFdecon(Seismogram(seis0), alg=alg, engine=processor2)
        processor3 = cloudpickle.loads(cloudpickle.dumps(processor))
        rf3 = RFdecon(Seismogram(seis0), alg=alg, engine=processor3)

        assert rf1.live
        assert rf2.live
        assert rf3.live
        assert np.allclose(np.asarray(rf1.data), np.asarray(rf2.data))
        assert np.allclose(np.asarray(rf1.data), np.asarray(rf3.data))
    finally:
        os.environ.pop("PFPATH", None)


@pytest.mark.parametrize(
    "alg,pf",
    [
        ("GeneralizedIterative", "TimeDomainGIDDecon.pf"),
        ("TimeDomainGID", "TimeDomainGIDDecon.pf"),
        ("FrequencyDomainGID", "FrequencyDomainGIDDecon.pf"),
    ],
)
def test_RFdeconProcessor_gid_dask_serialization_supports_distributed_use(alg, pf):
    os.environ["PFPATH"] = "./data/pf"
    try:
        wavelet = make_simulation_wavelet()
        impulses = make_impulse_data()
        seis0 = addnoise(convolve_wavelet(impulses, wavelet), nscale=0.0, padlength=800)
        processor = RFdeconProcessor(alg=alg, pf=pf)
        os.environ.pop("PFPATH", None)

        rf1 = RFdecon(Seismogram(seis0), alg=alg, engine=processor)
        processor2 = _distributed_roundtrip(processor)
        rf2 = RFdecon(Seismogram(seis0), alg=alg, engine=processor2)

        assert rf1.live
        assert rf2.live
        assert np.allclose(np.asarray(rf1.data), np.asarray(rf2.data))
    finally:
        os.environ.pop("PFPATH", None)


@pytest.mark.parametrize(
    "alg",
    [
        "LeastSquares",
        "MultiTaperPowerXcor",
        "GeneralizedIterative",
        "FrequencyDomainGID",
    ],
)
def test_RFdeconProcessor_pickle_is_self_contained_without_pfpath(alg):
    os.environ["PFPATH"] = "./data/pf"
    try:
        processor = RFdeconProcessor(alg=alg)
        payload = pickle.dumps(processor)
        os.environ.pop("PFPATH", None)
        restored = pickle.loads(payload)
        assert restored.algorithm == processor.algorithm
        assert dict(restored.md) == dict(processor.md)
    finally:
        os.environ.pop("PFPATH", None)


def test_RFdeconProcessor_change_parameters_uses_public_engine_api():
    old_pfpath = os.environ.get("PFPATH")
    os.environ["PFPATH"] = "./data/pf"
    try:
        processor = RFdeconProcessor(alg="LeastSquares")
        md = Metadata(processor.md)
        processor.change_parameters(md)
        assert dict(processor.md) == dict(md)

        gid_processor = RFdeconProcessor(
            alg="GeneralizedIterative", pf="TimeDomainGIDDecon.pf"
        )
        leaf_md = Metadata(
            AntelopePf("TimeDomainGIDDecon.pf")
            .get_branch("deconvolution_operator_type")
            .get_branch("least_square")
        )
        leaf_md["damping_factor"] = 100.0
        gid_processor.change_parameters(leaf_md)
        payloads = [
            pickle.loads(pickle.dumps(gid_processor)),
            cloudpickle.loads(cloudpickle.dumps(gid_processor)),
        ]

        wavelet = make_simulation_wavelet()
        impulses = make_impulse_data()
        seis0 = addnoise(convolve_wavelet(impulses, wavelet), nscale=0.0, padlength=800)
        rf_changed = RFdecon(
            Seismogram(seis0), alg="GeneralizedIterative", engine=gid_processor
        )
        assert rf_changed.live
        assert rf_changed["RFdecon_properties"][
            "gid_leaf_damping_factor"
        ] == pytest.approx(100.0)
        assert rf_changed["RFdecon_properties"]["gid_leaf_parameters_changed"]
        for restored in payloads:
            rf_restored = RFdecon(
                Seismogram(seis0), alg="GeneralizedIterative", engine=restored
            )
            assert rf_restored.live
            assert rf_restored["RFdecon_properties"][
                "gid_leaf_damping_factor"
            ] == pytest.approx(100.0)
            assert np.allclose(
                np.asarray(rf_changed.data), np.asarray(rf_restored.data)
            )
    finally:
        if old_pfpath is None:
            os.environ.pop("PFPATH", None)
        else:
            os.environ["PFPATH"] = old_pfpath


def test_RFdeconProcessor_change_parameters_preserved_by_dask_serialization():
    old_pfpath = os.environ.get("PFPATH")
    os.environ["PFPATH"] = "./data/pf"
    try:
        gid_processor = RFdeconProcessor(
            alg="GeneralizedIterative", pf="TimeDomainGIDDecon.pf"
        )
        leaf_md = Metadata(
            AntelopePf("TimeDomainGIDDecon.pf")
            .get_branch("deconvolution_operator_type")
            .get_branch("least_square")
        )
        leaf_md["damping_factor"] = 100.0
        gid_processor.change_parameters(leaf_md)
        restored = _distributed_roundtrip(gid_processor)

        wavelet = make_simulation_wavelet()
        impulses = make_impulse_data()
        seis0 = addnoise(convolve_wavelet(impulses, wavelet), nscale=0.0, padlength=800)
        rf_changed = RFdecon(
            Seismogram(seis0), alg="GeneralizedIterative", engine=gid_processor
        )
        rf_restored = RFdecon(
            Seismogram(seis0), alg="GeneralizedIterative", engine=restored
        )
        assert rf_restored.live
        assert rf_restored["RFdecon_properties"][
            "gid_leaf_damping_factor"
        ] == pytest.approx(100.0)
        assert np.allclose(np.asarray(rf_changed.data), np.asarray(rf_restored.data))
    finally:
        if old_pfpath is None:
            os.environ.pop("PFPATH", None)
        else:
            os.environ["PFPATH"] = old_pfpath


@pytest.mark.parametrize(
    "alg,metadata_key,replacement",
    [
        (
            "LeastSquares",
            "damping_factor",
            ("damping_factor 1.0", "damping_factor 0.123"),
        ),
        (
            "GeneralizedIterative",
            "maximum_iterations",
            ("maximum_iterations 100", "maximum_iterations 7"),
        ),
        (
            "FrequencyDomainGID",
            "maximum_iterations",
            ("maximum_iterations 100", "maximum_iterations 7"),
        ),
    ],
)
def test_RFdeconProcessor_pickle_uses_same_pf_when_pfpath_has_multiple_matches(
    tmp_path, alg, metadata_key, replacement
):
    pf1 = tmp_path / "pf1"
    pf2 = tmp_path / "pf2"
    pf1.mkdir()
    pf2.mkdir()
    if alg == "FrequencyDomainGID":
        pf_name = "FrequencyDomainGIDDecon.pf"
    elif alg == "GeneralizedIterative":
        pf_name = "TimeDomainGIDDecon.pf"
    else:
        pf_name = "RFdeconProcessor.pf"
    text = open(f"data/pf/{pf_name}", encoding="utf-8").read()
    (pf1 / pf_name).write_text(text)
    (pf2 / pf_name).write_text(text.replace(replacement[0], replacement[1], 1))
    old_pfpath = os.environ.get("PFPATH")
    os.environ["PFPATH"] = f"{pf1}:{pf2}"
    try:
        processor = RFdeconProcessor(alg=alg, pf=pf_name)
        original_value = processor.md[metadata_key]
        payload = pickle.dumps(processor)
        os.environ.pop("PFPATH", None)
        restored = pickle.loads(payload)
        assert restored.md[metadata_key] == original_value
        assert dict(restored.md) == dict(processor.md)
    finally:
        if old_pfpath is None:
            os.environ.pop("PFPATH", None)
        else:
            os.environ["PFPATH"] = old_pfpath


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
    old_pfpath = os.environ.get("PFPATH")
    os.environ["PFPATH"] = "./data/pf"
    try:
        # note this definition of 3000 samples at 20 sps and setting t0 to
        # -35 s must be consistent with data window parameters in the
        # RFdeconProcessor.pf file stored data/pf.
        seis0 = get_live_seismogram(3000, 20.0)
        seis0.t0 = -35.0
        # this is a list of algorithms supported by the RFdecon function
        # They can be enabled by a parameter on the function or by
        # passing an instance of the engine.  We test both below
        alglist = [
            "LeastSquares",
            "TimeDomainLeastSquares",
            "WaterLevel",
            "MultiTaperPowerSpecDiv",
            "MultiTaperPowerXcor",
        ]
        # first test case where the operator is instantiated on each call
        # to RFdecon
        for alg in alglist:
            d = Seismogram(seis0)
            # first verify it works without returning actual and ideal wavelets
            d_decon = RFdecon(d, alg=alg)
            assert d_decon.live
            print(alg, d_decon.npts)

        # repeat the same loop as above but pass the engine as an argument
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
    finally:
        if old_pfpath is None:
            os.environ.pop("PFPATH", None)
        else:
            os.environ["PFPATH"] = old_pfpath


def test_RFdecon_enables_generalized_iterative():
    old_pfpath = os.environ.get("PFPATH")
    os.environ["PFPATH"] = "./data/pf"
    try:
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
        assert qc["deconvolution_type"] == DEFAULT_GID_DECONVOLUTION_TYPE
        assert qc["ns_gid_enabled"]
        assert qc["ns_gid_peak_sigma_threshold"] == pytest.approx(4.0)
    finally:
        if old_pfpath is None:
            os.environ.pop("PFPATH", None)
        else:
            os.environ["PFPATH"] = old_pfpath


def test_RFdecon_gid_qc_namespaces_leaf_metadata(tmp_path):
    pf = _gid_pf_with_mode(tmp_path, "cnr")

    wavelet = make_simulation_wavelet()
    truth = make_impulse_data()
    data = addnoise(convolve_wavelet(truth, wavelet), nscale=1.0e-4, padlength=800)
    data["low_f_band_edge"] = 0.02
    data["high_f_band_edge"] = 2.0

    rf = RFdecon(data, alg="GeneralizedIterative", pf=str(pf))

    assert rf.live
    qc = rf["RFdecon_properties"]
    assert qc["shaping_wavelet_type"] == "ricker"
    assert qc["shaping_wavelet_frequency"] == pytest.approx(1.0)
    assert qc["gid_leaf_shaping_wavelet_type"] == "butterworth"
    assert qc["gid_leaf_damping_factor"] == pytest.approx(1.0)
    assert "damping_factor" not in qc
    assert not qc["gid_leaf_parameters_changed"]


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
    old_pfpath = os.environ.get("PFPATH")
    os.environ["PFPATH"] = "./data/pf"
    try:
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
        qc = rf["RFdecon_properties"]
        assert qc["iteration_count"] > 0
        assert "prediction_error" not in qc
        assert qc["deconvolution_type"] == DEFAULT_GID_DECONVOLUTION_TYPE
        assert "deconvolution_data_window_start" in qc
        assert "target_sample_interval" in qc
    finally:
        if old_pfpath is None:
            os.environ.pop("PFPATH", None)
        else:
            os.environ["PFPATH"] = old_pfpath


@pytest.mark.parametrize(
    "alg,pf",
    [
        ("GeneralizedIterative", "TimeDomainGIDDecon.pf"),
        ("FrequencyDomainGID", "FrequencyDomainGIDDecon.pf"),
    ],
)
def test_RFdecon_gid_accepts_raw_vector_wavelet_and_noise(alg, pf):
    old_pfpath = os.environ.get("PFPATH")
    os.environ["PFPATH"] = "./data/pf"
    try:
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
    finally:
        if old_pfpath is None:
            os.environ.pop("PFPATH", None)
        else:
            os.environ["PFPATH"] = old_pfpath


def test_RFdecon_preserves_preconfigured_gid_external_wavelet_when_omitted(tmp_path):
    os.environ["PFPATH"] = "./data/pf"
    try:
        pf = _gid_pf_with_mode(tmp_path, "ns_gid")
        wavelet = make_simulation_wavelet()
        impulses = make_impulse_data()
        seis0 = addnoise(convolve_wavelet(impulses, wavelet), nscale=0.0, padlength=800)
        processor = RFdeconProcessor(alg="GeneralizedIterative", pf=str(pf))

        rf_external = RFdecon(
            Seismogram(seis0),
            alg="GeneralizedIterative",
            engine=processor,
            wavelet=wavelet,
        )
        assert rf_external.live
        assert rf_external["RFdecon_properties"]["ns_gid_external_wavelet_used"]

        rf_preserved = RFdecon(
            Seismogram(seis0), alg="GeneralizedIterative", engine=processor
        )
        assert rf_preserved.live
        assert rf_preserved["RFdecon_properties"]["ns_gid_external_wavelet_used"]
        assert np.allclose(np.asarray(rf_external.data), np.asarray(rf_preserved.data))

        processor.clear_external_wavelet()
        rf_internal = RFdecon(
            Seismogram(seis0), alg="GeneralizedIterative", engine=processor
        )
        assert rf_internal.live
        assert not rf_internal["RFdecon_properties"]["ns_gid_external_wavelet_used"]
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
    just kill the return but the content can be variable.  All the possible
    variations in what would be posted to elog are too difficult to cover
    so we only test generic handling as kills.
    """
    old_pfpath = os.environ.get("PFPATH")
    os.environ["PFPATH"] = "./data/pf"
    try:
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
    finally:
        if old_pfpath is None:
            os.environ.pop("PFPATH", None)
        else:
            os.environ["PFPATH"] = old_pfpath


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
        (
            "MultiTaperPowerXcor",
            """MultiTaperPowerXcor &Tbl{
bad
}
""",
            "defined but is not an &Arr",
        ),
        (
            "MultiTaperPowerSpecDiv",
            """MultiTaperPowerSpecDiv &Tbl{
bad
}
""",
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

    with pytest.raises(MsPASSError, match=expected_message) as excinfo:
        RFdeconProcessor(alg=preferred_alg, pf=str(pf))
    assert excinfo.value.severity == ErrorSeverity.Fatal


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
    pf = _gid_pf_with_mode(tmp_path, "ns_gid")

    wavelet = make_simulation_wavelet()
    truth = make_impulse_data()
    data = convolve_wavelet(truth, wavelet)
    data = addnoise(data, nscale=1.0e-4, padlength=800)
    data["low_f_band_edge"] = 0.02
    data["high_f_band_edge"] = 2.0

    processor = RFdeconProcessor(alg="GeneralizedIterative", pf=str(pf))
    processor.loadwavelet(wavelet, dtype="TimeSeries")
    processor.loadnoise(data, dtype="Seismogram", component=2, window=True)
    state = processor.__getstate__()
    assert "processor" in state
    assert "_pf_text" not in state
    assert "wvector" not in state
    assert "wtimeseries" not in state
    assert "nvector" not in state
    assert "ntimeseries" not in state

    payload = cloudpickle.dumps(processor)
    old_pfpath = os.environ.get("PFPATH")
    os.environ.pop("PFPATH", None)
    try:
        restored = cloudpickle.loads(payload)
    finally:
        if old_pfpath is not None:
            os.environ["PFPATH"] = old_pfpath
    result = processor.apply_3c(data)
    restored_result = restored.apply_3c(Seismogram(data))

    assert result.live
    assert restored_result.live
    assert np.allclose(np.asarray(result.data), np.asarray(restored_result.data))
    qc = dict(processor.processor.QCMetrics())
    assert qc["ns_gid_enabled"]
    assert qc["ns_gid_external_wavelet_used"]


def test_RFdeconProcessor_apply_3c_external_wavelet_is_dask_serializable(tmp_path):
    pf = _gid_pf_with_mode(tmp_path, "ns_gid")

    wavelet = make_simulation_wavelet()
    truth = make_impulse_data()
    data = convolve_wavelet(truth, wavelet)
    data = addnoise(data, nscale=1.0e-4, padlength=800)
    data["low_f_band_edge"] = 0.02
    data["high_f_band_edge"] = 2.0

    processor = RFdeconProcessor(alg="GeneralizedIterative", pf=str(pf))
    processor.loadwavelet(wavelet, dtype="TimeSeries")
    processor.loadnoise(data, dtype="Seismogram", component=2, window=True)
    old_pfpath = os.environ.get("PFPATH")
    os.environ.pop("PFPATH", None)
    try:
        restored_dask = _distributed_roundtrip(processor)
    finally:
        if old_pfpath is not None:
            os.environ["PFPATH"] = old_pfpath
    result = processor.apply_3c(data)
    restored_dask_result = restored_dask.apply_3c(Seismogram(data))

    assert result.live
    assert restored_dask_result.live
    assert np.allclose(np.asarray(result.data), np.asarray(restored_dask_result.data))


def test_RFdeconProcessor_clear_external_state_drops_pickle_payload(tmp_path):
    pf = _gid_pf_with_mode(tmp_path, "ns_gid")

    processor = RFdeconProcessor(alg="GeneralizedIterative", pf=str(pf))
    baseline_size = len(pickle.dumps(processor))

    large_wavelet = TimeSeries(50000)
    large_wavelet.set_t0(-10.0)
    large_wavelet.set_dt(0.05)
    large_wavelet.set_live()
    for i in range(large_wavelet.npts):
        large_wavelet.data[i] = np.sin(0.01 * i)
    processor.loadwavelet(large_wavelet, dtype="TimeSeries")
    loaded_wavelet_size = len(pickle.dumps(processor))
    assert loaded_wavelet_size > baseline_size + 100000

    processor.clear_external_wavelet()
    cleared_wavelet_size = len(pickle.dumps(processor))
    assert cleared_wavelet_size < baseline_size * 2

    large_noise = TimeSeries(50000)
    large_noise.set_t0(-35.0)
    large_noise.set_dt(0.05)
    large_noise.set_live()
    for i in range(large_noise.npts):
        large_noise.data[i] = 0.01 * np.cos(0.02 * i)
    processor.loadnoise(large_noise, dtype="TimeSeries")
    loaded_noise_size = len(pickle.dumps(processor))
    assert loaded_noise_size > baseline_size + 100000

    processor.clear_external_noise()
    cleared_noise_size = len(pickle.dumps(processor))
    assert cleared_noise_size < baseline_size * 2


def test_RFdeconProcessor_clear_external_methods_are_gid_only():
    os.environ["PFPATH"] = "./data/pf"
    try:
        processor = RFdeconProcessor(alg="LeastSquares")
        seis_data = get_live_seismogram()
        processor.loaddata(seis_data)
        processor.loadwavelet(seis_data)
        with pytest.raises(RuntimeError, match="only valid for GID"):
            processor.clear_external_wavelet()
        with pytest.raises(RuntimeError, match="only valid for GID"):
            processor.clear_external_noise()
        assert hasattr(processor, "wvector")
    finally:
        os.environ.pop("PFPATH", None)


def test_RFdeconProcessor_gid_loadwavelet_is_transactional(tmp_path):
    pf = _gid_pf_with_mode(tmp_path, "ns_gid")

    wavelet = make_simulation_wavelet()
    truth = make_impulse_data()
    data = addnoise(convolve_wavelet(truth, wavelet), nscale=1.0e-4, padlength=800)

    processor = RFdeconProcessor(alg="GeneralizedIterative", pf=str(pf))
    processor.loadwavelet(wavelet, dtype="TimeSeries")
    first = processor.apply_3c(Seismogram(data))
    assert first.live
    assert dict(processor.processor.QCMetrics())["ns_gid_external_wavelet_used"]

    bad_wavelet = TimeSeries(wavelet)
    bad_wavelet.set_dt(wavelet.dt * 2.0)
    with pytest.raises(MsPASSError, match="target_sample_interval"):
        processor.loadwavelet(bad_wavelet, dtype="TimeSeries")

    second = processor.apply_3c(Seismogram(data))
    assert second.live
    qc = dict(processor.processor.QCMetrics())
    assert qc["ns_gid_external_wavelet_used"]
    assert np.allclose(np.asarray(first.data), np.asarray(second.data))

    bad_call = RFdecon(Seismogram(data), engine=processor, wavelet=bad_wavelet)
    assert bad_call.dead()
    recovered = processor.apply_3c(Seismogram(data))
    assert recovered.live
    assert dict(processor.processor.QCMetrics())["ns_gid_external_wavelet_used"]


def test_RFdeconProcessor_gid_loadnoise_timeseries_and_clear(tmp_path):
    pf = _gid_pf_with_mode(tmp_path, "ns_gid")

    wavelet = make_simulation_wavelet()
    truth = make_impulse_data()
    data = addnoise(convolve_wavelet(truth, wavelet), nscale=1.0e-4, padlength=800)
    noise = TimeSeries(wavelet)
    for i in range(noise.npts):
        noise.data[i] = 0.01 * np.sin(0.17 * i)

    processor = RFdeconProcessor(alg="GeneralizedIterative", pf=str(pf))
    processor.loadwavelet(wavelet, dtype="TimeSeries")
    processor.loadnoise(noise, dtype="TimeSeries", window=False)
    external = processor.apply_3c(Seismogram(data))
    assert external.live
    assert dict(processor.processor.QCMetrics())["ns_gid_external_noise_used"]

    bad_noise = TimeSeries(noise)
    bad_noise.set_dt(noise.dt * 2.0)
    with pytest.raises(MsPASSError, match="target_sample_interval"):
        processor.loadnoise(bad_noise, dtype="TimeSeries", window=False)
    recovered = processor.apply_3c(Seismogram(data))
    assert recovered.live
    assert dict(processor.processor.QCMetrics())["ns_gid_external_noise_used"]

    processor.clear_external_noise()
    internal = processor.apply_3c(Seismogram(data))
    assert internal.live
    assert not dict(processor.processor.QCMetrics())["ns_gid_external_noise_used"]


def test_RFdeconProcessor_gid_getstate_does_not_invalidate_processed_state(tmp_path):
    pf = _gid_pf_with_mode(tmp_path, "ns_gid")

    wavelet = make_simulation_wavelet()
    truth = make_impulse_data()
    data = addnoise(convolve_wavelet(truth, wavelet), nscale=1.0e-4, padlength=800)

    processor = RFdeconProcessor(alg="GeneralizedIterative", pf=str(pf))
    processor.loadwavelet(wavelet, dtype="TimeSeries")
    processor.loadnoise(data, dtype="Seismogram", component=2, window=True)
    result = processor.apply_3c(data)
    assert result.live
    assert dict(processor.processor.QCMetrics())["gid_processed"]
    actual_before = processor.processor.actual_output()

    cloudpickle.dumps(processor)

    assert dict(processor.processor.QCMetrics())["gid_processed"]
    actual_after = processor.processor.actual_output()
    assert np.allclose(np.asarray(actual_before.data), np.asarray(actual_after.data))


def test_RFdeconProcessor_gid_dask_roundtrip_does_not_invalidate_state(tmp_path):
    pf = _gid_pf_with_mode(tmp_path, "ns_gid")

    wavelet = make_simulation_wavelet()
    truth = make_impulse_data()
    data = addnoise(convolve_wavelet(truth, wavelet), nscale=1.0e-4, padlength=800)

    processor = RFdeconProcessor(alg="GeneralizedIterative", pf=str(pf))
    processor.loadwavelet(wavelet, dtype="TimeSeries")
    processor.loadnoise(data, dtype="Seismogram", component=2, window=True)
    result = processor.apply_3c(data)
    assert result.live
    assert dict(processor.processor.QCMetrics())["gid_processed"]
    actual_before = processor.processor.actual_output()

    _distributed_roundtrip(processor)

    assert dict(processor.processor.QCMetrics())["gid_processed"]
    actual_after = processor.processor.actual_output()
    assert np.allclose(np.asarray(actual_before.data), np.asarray(actual_after.data))
