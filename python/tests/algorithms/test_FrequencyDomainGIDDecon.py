#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest
import numpy as np
import pickle
import cloudpickle
from distributed.protocol import deserialize, serialize

from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.algorithms.deconvolution import FrequencyDomainGIDDecon
from mspasspy.ccore.utility import Metadata, MsPASSError, pfread
from mspasspy.ccore.seismic import DoubleVector, PowerSpectrum, Seismogram, TimeSeries
from mspasspy.algorithms.FrequencyDomainGIDDecon import FrequencyDomainGIDRFDecon

from test_TimeDomainGIDDecon import (
    _assert_actual_and_output_shaping_are_distinct,
    _assert_single_spike_recovery,
    _assert_valid_rf,
    _make_external_noise,
    _make_external_wavelet_3c_data,
    _make_gid_test_data,
    _make_single_spike_convolution_data,
    _ns_gid_pf,
    _pf_with_mode,
)


def _run_frequency_gid(data, pf):
    engine = FrequencyDomainGIDDecon(pf)
    return FrequencyDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
        return_wavelet=True,
    )


def test_FrequencyDomainGIDDecon_binding_and_wrapper():
    data = _make_gid_test_data(noise_level=None)
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")
    engine = FrequencyDomainGIDDecon(pf)

    rf, actual_output, output_shaping_wavelet = FrequencyDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-8.0, 20.0),
        noise_window=TimeWindow(-25.0, -8.0),
        return_wavelet=True,
    )

    _assert_valid_rf(rf)
    assert actual_output.live
    assert output_shaping_wavelet.live
    _assert_actual_and_output_shaping_are_distinct(
        actual_output, output_shaping_wavelet
    )
    assert rf.is_defined("FrequencyDomainGIDDecon_properties")
    qc = rf["FrequencyDomainGIDDecon_properties"]
    assert qc["iteration_count"] > 0
    assert qc["residual_L2_final"] < qc["residual_L2_initial"]


def test_FrequencyDomainGIDDecon_engine_is_pickleable_for_wrapper_use():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")
    engine = FrequencyDomainGIDDecon(pf)
    restored = pickle.loads(pickle.dumps(engine))

    rf1 = FrequencyDomainGIDRFDecon(
        Seismogram(data),
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    rf2 = FrequencyDomainGIDRFDecon(
        Seismogram(data),
        restored,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )

    assert rf1.live
    assert rf2.live
    assert np.allclose(np.asarray(rf1.data), np.asarray(rf2.data))

    restored_after_use = pickle.loads(pickle.dumps(engine))
    rf3 = FrequencyDomainGIDRFDecon(
        Seismogram(data),
        restored_after_use,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    assert rf3.live
    assert np.allclose(np.asarray(rf1.data), np.asarray(rf3.data))

    restored_cloudpickle = cloudpickle.loads(cloudpickle.dumps(engine))
    rf4 = FrequencyDomainGIDRFDecon(
        Seismogram(data),
        restored_cloudpickle,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    assert rf4.live
    assert np.allclose(np.asarray(rf1.data), np.asarray(rf4.data))

    header, frames = serialize(engine)
    restored_dask = deserialize(header, frames)
    rf5 = FrequencyDomainGIDRFDecon(
        Seismogram(data),
        restored_dask,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    assert rf5.live
    assert np.allclose(np.asarray(rf1.data), np.asarray(rf5.data))

    changed = FrequencyDomainGIDDecon(pf)
    leaf_md = pf.get_branch("deconvolution_operator_type").get_branch("least_square")
    leaf_md["damping_factor"] = 100.0
    changed.changeparameter(leaf_md)
    rf_changed = FrequencyDomainGIDRFDecon(
        Seismogram(data),
        changed,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    for restored_changed in (
        pickle.loads(pickle.dumps(changed)),
        cloudpickle.loads(cloudpickle.dumps(changed)),
        deserialize(*serialize(changed)),
    ):
        rf_restored = FrequencyDomainGIDRFDecon(
            Seismogram(data),
            restored_changed,
            signal_window=TimeWindow(-10.0, 20.0),
            noise_window=TimeWindow(-35.0, -5.0),
        )
        assert rf_restored.live
        assert np.allclose(np.asarray(rf_changed.data), np.asarray(rf_restored.data))


def test_FrequencyDomainGIDDecon_pickle_preserves_external_wavelet_and_noise(tmp_path):
    data, wavelet, _ = _make_external_wavelet_3c_data(noise_level=2.0e-4)
    noise = _make_external_noise()
    pf = _ns_gid_pf(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
    )
    engine = FrequencyDomainGIDDecon(pf)
    engine.loadwavelet(wavelet)
    engine.loadnoise(noise)
    restored = pickle.loads(pickle.dumps(engine))

    rf = FrequencyDomainGIDRFDecon(
        data,
        restored,
        signal_window=TimeWindow(-8.0, 22.0),
        noise_window=TimeWindow(-35.0, -8.0),
    )

    assert rf.live
    qc = rf["FrequencyDomainGIDDecon_properties"]
    assert qc["ns_gid_external_wavelet_used"]
    assert qc["ns_gid_external_noise_used"]

    spectrum_engine = FrequencyDomainGIDDecon(pf)
    spectrum_engine.loadwavelet(wavelet)
    spectrum_engine.loadnoise(
        PowerSpectrum(
            Metadata(), DoubleVector([1.0, 1.0, 1.0]), 1.0, "valid", -1.0, 1.0, 3
        )
    )
    restored_spectrum = pickle.loads(pickle.dumps(spectrum_engine))
    rf_spectrum = FrequencyDomainGIDRFDecon(
        data,
        restored_spectrum,
        signal_window=TimeWindow(-8.0, 22.0),
        noise_window=TimeWindow(-35.0, -8.0),
    )
    assert rf_spectrum.live
    qc_spectrum = rf_spectrum["FrequencyDomainGIDDecon_properties"]
    assert qc_spectrum["ns_gid_external_wavelet_used"]
    assert qc_spectrum["ns_gid_external_noise_spectrum_used"]


def test_FrequencyDomainGIDDecon_powerspectrum_noise_still_requires_residual_noise(
    tmp_path,
):
    data = _make_single_spike_convolution_data()
    pf = _ns_gid_pf(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
    )
    engine = FrequencyDomainGIDDecon(pf)
    engine.loadnoise(
        PowerSpectrum(
            Metadata(), DoubleVector([1.0, 1.0, 1.0]), 1.0, "valid", -1.0, 1.0, 3
        )
    )
    assert engine.load(data, TimeWindow(-10.0, 20.0)) == 0
    with pytest.raises(MsPASSError, match="valid noise window has not been loaded"):
        engine.process()


def test_FrequencyDomainGIDDecon_validates_single_spike_recovery():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")

    rf, actual_output, output_shaping_wavelet = _run_frequency_gid(data, pf)

    _assert_valid_rf(rf)
    assert actual_output.live
    assert output_shaping_wavelet.live
    _assert_actual_and_output_shaping_are_distinct(
        actual_output, output_shaping_wavelet
    )
    qc = rf["FrequencyDomainGIDDecon_properties"]
    assert qc["iteration_count"] > 0
    assert qc["residual_Linf_final"] < qc["residual_Linf_initial"]
    assert qc["residual_L2_final"] < qc["residual_L2_initial"]

    _assert_single_spike_recovery(rf, ratio_tolerance=5.0e-2)


def test_FrequencyDomainGIDDecon_rejects_leaf_window_drift(tmp_path):
    text = open("data/pf/FrequencyDomainGIDDecon.pf", encoding="utf-8").read()
    text = text.replace(
        "least_square &Arr{\n        target_sample_interval 0.05\n        operator_nfft 512\n        deconvolution_data_window_start -5.0",
        "least_square &Arr{\n        target_sample_interval 0.05\n        operator_nfft 512\n        deconvolution_data_window_start -4.0",
    )
    pfname = tmp_path / "FrequencyDomainGIDDecon_bad_leaf.pf"
    pfname.write_text(text)

    with pytest.raises(MsPASSError, match="not consistent with GID parameters"):
        FrequencyDomainGIDDecon(pfread(str(pfname)))


def test_FrequencyDomainNSGID_uses_external_wavelet_and_gain_cap(tmp_path):
    data, wavelet, spike_times = _make_external_wavelet_3c_data(noise_level=2.0e-4)
    pf = _ns_gid_pf(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
        gain_max=30.0,
    )
    engine = FrequencyDomainGIDDecon(pf)
    rf = FrequencyDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-8.0, 22.0),
        noise_window=TimeWindow(-35.0, -8.0),
        external_wavelet=wavelet,
    )

    _assert_valid_rf(rf)
    qc = rf["FrequencyDomainGIDDecon_properties"]
    assert qc["ns_gid_enabled"]
    assert qc["ns_gid_external_wavelet_used"]
    assert qc["ns_gid_gain_max_actual"] <= 30.0 * (1.0 + 1.0e-10)
    assert qc["ns_gid_number_spikes"] <= 2 * len(spike_times)
    support = np.where(np.linalg.norm(np.asarray(rf.data), axis=0) > 1.0e-8)[0]
    picked_times = [rf.time(int(i)) for i in support]
    assert picked_times
    expected_times = [t - wavelet.t0 for t in spike_times[:2]]
    for t in expected_times:
        assert min(abs(t - p) for p in picked_times) < 0.15


def test_FrequencyDomainGIDRFDecon_preserves_engine_external_state_between_calls(tmp_path):
    data = _make_gid_test_data(noise_level=1.0e-4)
    from decon_data_generators import make_simulation_wavelet

    external_wavelet = make_simulation_wavelet()
    pf = _ns_gid_pf(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
    )
    engine = FrequencyDomainGIDDecon(pf)

    rf_external = FrequencyDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-8.0, 20.0),
        noise_window=TimeWindow(-25.0, -8.0),
        external_wavelet=external_wavelet,
    )
    assert rf_external.live
    assert rf_external["FrequencyDomainGIDDecon_properties"][
        "ns_gid_external_wavelet_used"
    ]

    rf_internal = FrequencyDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-8.0, 20.0),
        noise_window=TimeWindow(-25.0, -8.0),
    )
    assert rf_internal.live
    assert rf_internal["FrequencyDomainGIDDecon_properties"][
        "ns_gid_external_wavelet_used"
    ]


def test_FrequencyDomainGIDDecon_rejects_empty_external_noise(tmp_path):
    from mspasspy.ccore.seismic import TimeSeries

    pf = _ns_gid_pf(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
    )
    engine = FrequencyDomainGIDDecon(pf)
    empty_noise = TimeSeries(0)
    empty_noise.set_live()

    with pytest.raises(MsPASSError, match="external noise is empty"):
        engine.loadnoise(empty_noise)


def test_FrequencyDomainGIDDecon_rejects_invalid_external_noise_spectrum(tmp_path):
    pf = _ns_gid_pf(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
    )
    engine = FrequencyDomainGIDDecon(pf)
    dead_spectrum = PowerSpectrum(
        Metadata(), DoubleVector([1.0]), 1.0, "dead", 0.0, 1.0, 1
    )
    dead_spectrum.kill()
    live_empty_spectrum = PowerSpectrum(
        Metadata(), DoubleVector([]), 1.0, "empty", 0.0, 1.0, 0
    )
    one_bin_spectrum = PowerSpectrum(
        Metadata(), DoubleVector([1.0]), 1.0, "one_bin", 0.0, 1.0, 1
    )
    missing_dc_spectrum = PowerSpectrum(
        Metadata(), DoubleVector([1.0, 1.0]), 1.0, "missing_dc", 1.0, 1.0, 2
    )
    below_dc_spectrum = PowerSpectrum(
        Metadata(), DoubleVector([1.0, 1.0]), 1.0, "below_dc", -10.0, 1.0, 2
    )
    dc_at_last_bin_spectrum = PowerSpectrum(
        Metadata(), DoubleVector([1.0, 1.0]), 1.0, "dc_at_last", -1.0, 1.0, 2
    )

    with pytest.raises(MsPASSError, match="PowerSpectrum is marked dead"):
        engine.loadnoise(dead_spectrum)
    with pytest.raises(MsPASSError, match="at least two frequency bins"):
        engine.loadnoise(live_empty_spectrum)
    with pytest.raises(MsPASSError, match="at least two frequency bins"):
        engine.loadnoise(one_bin_spectrum)
    with pytest.raises(MsPASSError, match="cover DC"):
        engine.loadnoise(missing_dc_spectrum)
    with pytest.raises(MsPASSError, match="cover DC"):
        engine.loadnoise(below_dc_spectrum)
    with pytest.raises(MsPASSError, match="cover DC"):
        engine.loadnoise(dc_at_last_bin_spectrum)


@pytest.mark.parametrize("mode", ["multi_taper", "least_square", "water_level", "cnr"])
def test_FrequencyDomainGIDDecon_rejects_non_ns_power_spectrum_noise(tmp_path, mode):
    pf = _pf_with_mode(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
        mode,
    )
    engine = FrequencyDomainGIDDecon(pf)
    spectrum = PowerSpectrum(
        Metadata(), DoubleVector([1.0, 1.0, 1.0]), 1.0, "valid", -1.0, 1.0, 3
    )

    with pytest.raises(MsPASSError, match="only supported for ns_gid"):
        engine.loadnoise(spectrum)


def test_FrequencyDomainGIDDecon_rejects_dead_external_wavelet_and_noise(tmp_path):
    pf = _ns_gid_pf(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
    )
    engine = FrequencyDomainGIDDecon(pf)
    dead_wavelet = TimeSeries(8)
    dead_wavelet.kill()
    dead_noise = TimeSeries(8)
    dead_noise.kill()

    with pytest.raises(MsPASSError, match="external wavelet is marked dead"):
        engine.loadwavelet(dead_wavelet)
    with pytest.raises(MsPASSError, match="external noise is marked dead"):
        engine.loadnoise(dead_noise)


def test_FrequencyDomainGIDDecon_rejects_external_timeseries_dt_mismatch(tmp_path):
    pf = _ns_gid_pf(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
    )
    engine = FrequencyDomainGIDDecon(pf)
    wavelet = TimeSeries(8)
    wavelet.set_live()
    wavelet.set_dt(0.2)
    noise = TimeSeries(8)
    noise.set_live()
    noise.set_dt(0.2)

    with pytest.raises(MsPASSError, match="target_sample_interval"):
        engine.loadwavelet(wavelet)
    with pytest.raises(MsPASSError, match="target_sample_interval"):
        engine.loadnoise(noise)


@pytest.mark.parametrize("mode", ["least_square", "water_level", "multi_taper", "cnr"])
def test_FrequencyDomainGIDDecon_inverse_modes_are_valid(tmp_path, mode):
    data = _make_single_spike_convolution_data()
    pf = _pf_with_mode(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
        mode,
    )

    rf, actual_output, output_shaping_wavelet = _run_frequency_gid(data, pf)

    _assert_valid_rf(rf)
    assert actual_output.live
    assert output_shaping_wavelet.live
    _assert_actual_and_output_shaping_are_distinct(
        actual_output, output_shaping_wavelet
    )
    qc = rf["FrequencyDomainGIDDecon_properties"]
    assert qc["iteration_count"] > 0
    assert qc["residual_L2_final"] < qc["residual_L2_initial"]
    _assert_single_spike_recovery(rf, ratio_tolerance=5.0e-2)


def test_FrequencyDomainGIDDecon_changeparameter_rejects_gid_level_metadata():
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")
    engine = FrequencyDomainGIDDecon(pf)
    gid_md = pf.get_branch("deconvolution_operator_type").get_branch(
        "frequency_domain_gid_deconvolution"
    )

    with pytest.raises(MsPASSError, match="GID-level"):
        engine.changeparameter(gid_md)


def test_FrequencyDomainGIDDecon_changeparameter_handles_cnr_mode(tmp_path):
    pf = _pf_with_mode(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
        "cnr",
    )
    engine = FrequencyDomainGIDDecon(pf)
    cnr_md = pf.get_branch("deconvolution_operator_type").get_branch("cnr")

    engine.changeparameter(cnr_md)


def test_FrequencyDomainGIDDecon_preprocess_outputs_are_guarded():
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")
    engine = FrequencyDomainGIDDecon(pf)
    qc = dict(engine.QCMetrics())

    assert qc["gid_processed"] is False
    assert qc["iteration_count"] == 0
    assert qc["residual_Linf_final"] == 0.0
    assert qc["residual_L2_final"] == 0.0
    for method in (
        engine.getresult,
        engine.sparse_output,
        engine.actual_output,
        engine.inverse_wavelet,
    ):
        with pytest.raises(MsPASSError, match="process must be called first"):
            method()


def test_FrequencyDomainGIDDecon_changeparameter_invalidates_outer_state():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")
    engine = FrequencyDomainGIDDecon(pf)
    FrequencyDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    assert dict(engine.QCMetrics())["gid_processed"] is True

    leaf_md = pf.get_branch("deconvolution_operator_type").get_branch("least_square")
    engine.changeparameter(leaf_md)

    qc = dict(engine.QCMetrics())
    assert qc["gid_processed"] is False
    assert qc["iteration_count"] == 0
    with pytest.raises(MsPASSError, match="process must be called first"):
        engine.getresult()
    with pytest.raises(MsPASSError, match="process must be called first"):
        engine.actual_output()


def test_FrequencyDomainGIDDecon_failed_changeparameter_invalidates_without_poisoning_leaf():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")
    engine = FrequencyDomainGIDDecon(pf)
    FrequencyDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    assert dict(engine.QCMetrics())["gid_processed"] is True

    leaf_md = pf.get_branch("deconvolution_operator_type").get_branch("least_square")
    leaf_md["damping_factor"] = -1.0
    with pytest.raises(MsPASSError, match="damping_factor"):
        engine.changeparameter(leaf_md)

    assert dict(engine.QCMetrics())["gid_processed"] is False
    with pytest.raises(MsPASSError, match="process must be called first"):
        engine.getresult()
    engine.process()
    assert dict(engine.QCMetrics())["gid_processed"] is True


def test_FrequencyDomainGIDDecon_load_invalidates_outer_state():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")
    engine = FrequencyDomainGIDDecon(pf)
    FrequencyDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    assert dict(engine.QCMetrics())["gid_processed"] is True

    assert engine.load(data, TimeWindow(-10.0, 20.0)) == 0

    assert dict(engine.QCMetrics())["gid_processed"] is False
    with pytest.raises(MsPASSError, match="process must be called first"):
        engine.getresult()


def test_FrequencyDomainGIDDecon_failed_load_invalidates_and_clears_old_data():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")
    engine = FrequencyDomainGIDDecon(pf)
    FrequencyDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    assert dict(engine.QCMetrics())["gid_processed"] is True

    assert engine.load(data, TimeWindow(30.0, 40.0)) == 1

    assert dict(engine.QCMetrics())["gid_processed"] is False
    with pytest.raises(MsPASSError, match="process must be called first"):
        engine.getresult()
    with pytest.raises(MsPASSError, match="valid data window"):
        engine.process()


def test_FrequencyDomainGIDDecon_failed_combined_load_invalidates_old_output():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")
    engine = FrequencyDomainGIDDecon(pf)
    FrequencyDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    assert dict(engine.QCMetrics())["gid_processed"] is True

    assert engine.load(data, TimeWindow(30.0, 40.0), TimeWindow(-35.0, -5.0)) == 1

    assert dict(engine.QCMetrics())["gid_processed"] is False
    with pytest.raises(MsPASSError, match="process must be called first"):
        engine.getresult()
    with pytest.raises(MsPASSError, match="valid data window"):
        engine.process()


def test_FrequencyDomainGIDDecon_combined_load_preserves_external_noise_until_clear(
    tmp_path,
):
    data = _make_single_spike_convolution_data()
    pf = _ns_gid_pf(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
        gain_max=20.0,
    )
    dwin = TimeWindow(-10.0, 20.0)
    nwin = TimeWindow(-35.0, -5.0)
    external_noise = TimeSeries(300)
    external_noise.set_t0(-35.0)
    external_noise.set_dt(0.05)
    external_noise.set_live()
    for i in range(external_noise.npts):
        external_noise.data[i] = 100.0 * np.sin(0.11 * i)

    windowed_engine = FrequencyDomainGIDDecon(pf)
    assert windowed_engine.load(data, dwin, nwin) == 0
    windowed_engine.process()

    switched_engine = FrequencyDomainGIDDecon(pf)
    switched_engine.loadnoise(external_noise)
    assert switched_engine.load(data, dwin, nwin) == 0
    switched_engine.process()
    switched_qc = dict(switched_engine.QCMetrics())
    assert switched_qc["ns_gid_external_noise_used"]

    switched_engine.clear_external_noise()
    assert switched_engine.load(data, dwin, nwin) == 0
    switched_engine.process()

    windowed_qc = dict(windowed_engine.QCMetrics())
    switched_qc = dict(switched_engine.QCMetrics())
    assert not switched_qc["ns_gid_external_noise_used"]
    assert np.isclose(
        switched_qc["ns_gid_noise_amplification"],
        windowed_qc["ns_gid_noise_amplification"],
    )
    assert np.isclose(
        switched_qc["ns_gid_gain_max_actual"],
        windowed_qc["ns_gid_gain_max_actual"],
    )
    assert np.allclose(
        np.asarray(switched_engine.getresult().data),
        np.asarray(windowed_engine.getresult().data),
    )


def test_FrequencyDomainGIDDecon_replacing_external_noise_refreshes_threshold(
    tmp_path,
):
    data = _make_single_spike_convolution_data()
    pf = _ns_gid_pf(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
    )
    dwin = TimeWindow(-10.0, 20.0)
    low_noise = _make_external_noise(scale=1.0)
    high_noise = _make_external_noise(scale=1000.0)

    engine = FrequencyDomainGIDDecon(pf)
    engine.loadnoise(low_noise)
    assert engine.load(data, dwin) == 0
    engine.process()
    low_threshold = dict(engine.QCMetrics())["ns_gid_peak_threshold"]

    engine.loadnoise(high_noise)
    assert engine.load(data, dwin) == 0
    engine.process()
    high_threshold = dict(engine.QCMetrics())["ns_gid_peak_threshold"]

    assert high_threshold > 10.0 * low_threshold


def test_FrequencyDomainGIDDecon_failed_external_noise_replacement_preserves_state(
    tmp_path,
):
    data = _make_single_spike_convolution_data()
    pf = _ns_gid_pf(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
    )
    dwin = TimeWindow(-10.0, 20.0)
    noise = _make_external_noise(scale=2.0)

    engine = FrequencyDomainGIDDecon(pf)
    engine.loadnoise(noise)
    assert engine.load(data, dwin) == 0
    engine.process()
    original_qc = dict(engine.QCMetrics())

    bad_noise = TimeSeries(noise)
    bad_noise.set_dt(noise.dt * 2.0)
    with pytest.raises(MsPASSError, match="target_sample_interval"):
        engine.loadnoise(bad_noise)

    assert engine.load(data, dwin) == 0
    engine.process()
    recovered_qc = dict(engine.QCMetrics())
    assert recovered_qc["ns_gid_external_noise_used"]
    assert np.isclose(
        recovered_qc["ns_gid_peak_threshold"],
        original_qc["ns_gid_peak_threshold"],
    )


def test_FrequencyDomainGIDDecon_changeparameter_rejects_leaf_window_drift():
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")
    engine = FrequencyDomainGIDDecon(pf)
    leaf_md = pf.get_branch("deconvolution_operator_type").get_branch("least_square")
    leaf_md["deconvolution_data_window_start"] = (
        leaf_md.get_double("deconvolution_data_window_start") + 1.0
    )

    with pytest.raises(MsPASSError, match="does not match"):
        engine.changeparameter(leaf_md)


def test_FrequencyDomainGIDDecon_changeparameter_rejects_leaf_dt_drift():
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")
    engine = FrequencyDomainGIDDecon(pf)
    leaf_md = pf.get_branch("deconvolution_operator_type").get_branch("least_square")
    leaf_md["target_sample_interval"] = 0.1

    with pytest.raises(MsPASSError, match="target_sample_interval"):
        engine.changeparameter(leaf_md)


def test_FrequencyDomainGIDDecon_changeparameter_rejects_leaf_shaping_dt_drift():
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")
    engine = FrequencyDomainGIDDecon(pf)
    leaf_md = pf.get_branch("deconvolution_operator_type").get_branch("least_square")
    leaf_md["shaping_wavelet_dt"] = 0.1

    with pytest.raises(MsPASSError, match="shaping_wavelet_dt"):
        engine.changeparameter(leaf_md)


@pytest.mark.parametrize(
    "key,value",
    [
        ("residual_fractional_improvement_floor", 1.0e-3),
        ("ns_gid_refit_interval", 2),
        ("lag_weight_penalty_scale_factor", 0.5),
        ("lag_weight_function_width", 5),
        ("noise_window_start", -30.0),
        ("noise_window_end", -3.0),
    ],
)
def test_FrequencyDomainGIDDecon_changeparameter_rejects_gid_keys_on_leaf(
    key, value
):
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")
    engine = FrequencyDomainGIDDecon(pf)
    leaf_md = pf.get_branch("deconvolution_operator_type").get_branch("least_square")
    leaf_md[key] = value

    with pytest.raises(MsPASSError, match=key):
        engine.changeparameter(leaf_md)


def test_FrequencyDomainGIDRFDecon_argument_validation():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")
    engine = FrequencyDomainGIDDecon(pf)
    impl = FrequencyDomainGIDRFDecon.__wrapped__

    with pytest.raises(TypeError):
        impl("not a seismogram", engine)
    with pytest.raises(TypeError):
        impl(data, object())
    with pytest.raises(TypeError):
        impl(data, engine, signal_window=(-8.0, 20.0))
    with pytest.raises(TypeError):
        impl(data, engine, noise_window=(-25.0, -8.0))

    dead = data
    dead.kill()
    assert impl(dead, engine).dead()
    dead_result = impl(dead, engine, return_wavelet=True)
    assert dead_result[0].dead()
    assert dead_result[1] is None
    assert dead_result[2] is None


def test_FrequencyDomainGIDRFDecon_error_return_and_optional_qc():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")

    zero_data = Seismogram(data)
    zero_data.data[:, :] = 0.0
    engine = FrequencyDomainGIDDecon(pf)
    bad_result = FrequencyDomainGIDRFDecon(
        zero_data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
        return_wavelet=True,
    )
    assert bad_result[0].dead()
    assert bad_result[1] is None
    assert bad_result[2] is None

    engine = FrequencyDomainGIDDecon(pf)
    bad_window_result = FrequencyDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-2.0, 2.0),
        noise_window=TimeWindow(-35.0, -5.0),
        return_wavelet=True,
    )
    assert bad_window_result[0].dead()
    assert bad_window_result[1] is None
    assert bad_window_result[2] is None

    engine = FrequencyDomainGIDDecon(pf)
    rf = FrequencyDomainGIDRFDecon(data, engine, QCdata_key=None)
    _assert_valid_rf(rf)
    assert not rf.is_defined("FrequencyDomainGIDDecon_properties")
