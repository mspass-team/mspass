import os
import pickle
from pathlib import Path
import subprocess
import sys

import dask
import numpy as np
import pytest

from distributed import Client, LocalCluster
from distributed.protocol import deserialize, serialize

from mspasspy.ccore.seismic import (
    Seismogram,
    SeismogramEnsemble,
    TimeReferenceType,
    TimeSeries,
    TimeSeriesEnsemble,
)
from mspasspy.ccore.utility import AtomicType, ErrorSeverity


class TimeSeriesEnsembleSubclass(TimeSeriesEnsemble):
    pass


def _make_ensemble(ensemble_type, member_count=3, npts=4096, live=True):
    if ensemble_type is TimeSeriesEnsemble:
        member_type = TimeSeries
        atomic_type = AtomicType.TIMESERIES
    else:
        member_type = Seismogram
        atomic_type = AtomicType.SEISMOGRAM

    ensemble = ensemble_type(member_count)
    ensemble["ensemble_marker"] = ensemble_type.__name__
    ensemble["clean_metadata"] = "unchanged"
    ensemble["modified_metadata"] = "before"
    ensemble.clear_modified()
    ensemble["modified_metadata"] = "after"
    ensemble.elog.set_job_id(73)
    ensemble.elog.log_error(
        "ensemble-test", "preserve this message", ErrorSeverity.Complaint
    )
    for index in range(member_count):
        member = member_type(npts)
        member.dt = 0.05
        member.t0 = float(index)
        member.tref = (
            TimeReferenceType.UTC if index % 2 == 0 else TimeReferenceType.Relative
        )
        member["member_index"] = index
        member.set_as_origin("serializer-test", "1", f"member-{index}", atomic_type)
        member.set_live()
        if member_type is TimeSeries:
            member.data[0] = index + 0.25
            member.data[npts - 1] = index + 0.75
        else:
            member.data[0, 0] = index + 0.25
            member.data[2, npts - 1] = index + 0.75
        if index % 2:
            member.kill()
        ensemble.member.append(member)
    if live:
        ensemble.set_live()
    else:
        ensemble.kill()
    return ensemble


def _assert_ensemble_equal(actual, expected, compare_process_ids=True):
    assert type(actual) is type(expected)
    assert actual["ensemble_marker"] == expected["ensemble_marker"]
    assert actual["clean_metadata"] == expected["clean_metadata"]
    assert actual["modified_metadata"] == expected["modified_metadata"]
    assert set(actual.modified()) == set(expected.modified())
    assert actual.live is expected.live
    assert len(actual.member) == len(expected.member)
    assert actual.elog.get_job_id() == expected.elog.get_job_id()
    assert len(actual.elog) == len(expected.elog)
    for actual_log, expected_log in zip(
        actual.elog.get_error_log(), expected.elog.get_error_log()
    ):
        assert actual_log.job_id == expected_log.job_id
        if compare_process_ids:
            assert actual_log.p_id == expected_log.p_id
        assert actual_log.algorithm == expected_log.algorithm
        assert actual_log.message == expected_log.message
        assert actual_log.badness == expected_log.badness
    for index, (actual_member, expected_member) in enumerate(
        zip(actual.member, expected.member)
    ):
        assert actual_member["member_index"] == index
        assert actual_member.live == expected_member.live
        assert actual_member.dt == expected_member.dt
        assert actual_member.t0 == expected_member.t0
        assert actual_member.tref == expected_member.tref
        assert actual_member.number_of_stages() == expected_member.number_of_stages()
        assert (
            actual_member.current_nodedata().uuid
            == expected_member.current_nodedata().uuid
        )
        np.testing.assert_array_equal(actual_member.data, expected_member.data)


def _run_repository_subprocess(source):
    repository_root = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repository_root / "python")
    return subprocess.run(
        [sys.executable, "-c", source],
        cwd=repository_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )


def _uses_legacy_nested_member_pickle(ensemble):
    state = ensemble.__getstate__()
    return len(state) > 3 and isinstance(state[3], bytes)


@pytest.mark.parametrize(
    ("ensemble_type", "sample_bytes"),
    [
        (TimeSeriesEnsemble, 4096 * 8),
        (SeismogramEnsemble, 3 * 4096 * 8),
    ],
)
@pytest.mark.parametrize("live", [True, False])
def test_frame_serialization_roundtrip(ensemble_type, sample_bytes, live):
    ensemble = _make_ensemble(ensemble_type, live=live)

    header, frames = serialize(ensemble, on_error="raise")
    pickle_header, pickle_frames = serialize(
        ensemble, serializers=("pickle",), on_error="raise"
    )

    assert header["serializer"] == "dask"
    assert header["sub-header"]["version"] == 1
    assert len(frames) == len(ensemble.member) + 1
    assert [memoryview(frame).nbytes for frame in frames[1:]] == [sample_bytes] * len(
        ensemble.member
    )
    assert memoryview(frames[0]).nbytes < sample_bytes
    assert pickle_header["serializer"] == "pickle"
    if _uses_legacy_nested_member_pickle(ensemble):
        assert len(pickle_frames) == 1
        assert memoryview(pickle_frames[0]).nbytes > memoryview(frames[0]).nbytes
    _assert_ensemble_equal(deserialize(header, frames), ensemble)
    _assert_ensemble_equal(deserialize(pickle_header, pickle_frames), ensemble)


@pytest.mark.parametrize("ensemble_type", [TimeSeriesEnsemble, SeismogramEnsemble])
def test_empty_ensemble_roundtrip(ensemble_type):
    ensemble = ensemble_type()
    ensemble["ensemble_marker"] = ensemble_type.__name__

    header, frames = serialize(ensemble, on_error="raise")
    restored = deserialize(header, frames)

    assert header["serializer"] == "dask"
    assert len(frames) == 1
    assert type(restored) is ensemble_type
    assert restored.dead()
    assert len(restored.member) == 0
    assert restored["ensemble_marker"] == ensemble_type.__name__


def test_list_of_ensembles_roundtrip():
    ensembles = [
        _make_ensemble(TimeSeriesEnsemble, member_count=1, npts=64),
        _make_ensemble(SeismogramEnsemble, member_count=1, npts=64),
    ]

    header, frames = serialize(ensembles, on_error="raise")
    restored = deserialize(header, frames)

    assert header["is-collection"] is True
    assert [item["serializer"] for item in header["sub-headers"]] == [
        "dask",
        "dask",
    ]
    for actual, expected in zip(restored, ensembles):
        _assert_ensemble_equal(actual, expected)


@pytest.mark.parametrize("ensemble_type", [TimeSeriesEnsemble, SeismogramEnsemble])
def test_native_and_pickle_only_formats_are_unchanged(ensemble_type):
    ensemble = _make_ensemble(ensemble_type, member_count=1, npts=64)
    native_state_before = pickle.dumps(ensemble.__getstate__(), protocol=5)
    native_before = pickle.dumps(ensemble, protocol=5)

    header, frames = serialize(ensemble, serializers=("pickle",), on_error="raise")
    assert pickle.dumps(ensemble.__getstate__(), protocol=5) == native_state_before
    assert pickle.dumps(ensemble, protocol=5) == native_before
    assert header["serializer"] == "pickle"
    if _uses_legacy_nested_member_pickle(ensemble):
        assert len(frames) == 1
    _assert_ensemble_equal(deserialize(header, frames), ensemble)


def test_first_dask_registration_does_not_change_native_pickle_bytes():
    result = _run_repository_subprocess("""
import pickle

from distributed.protocol import serialize
from mspasspy.ccore.seismic import SeismogramEnsemble, TimeSeriesEnsemble

ensembles = [TimeSeriesEnsemble(), SeismogramEnsemble()]
state_before = [pickle.dumps(item.__getstate__(), protocol=5) for item in ensembles]
pickle_before = [pickle.dumps(item, protocol=5) for item in ensembles]

# The first Dask dispatch invokes mspasspy's lazy registration callback.
headers = [serialize(item, on_error="raise")[0] for item in ensembles]

assert [header["serializer"] for header in headers] == ["dask", "dask"]
assert [pickle.dumps(item.__getstate__(), protocol=5) for item in ensembles] == state_before
assert [pickle.dumps(item, protocol=5) for item in ensembles] == pickle_before
""")
    assert result.returncode == 0, result.stderr


def test_python_subclass_falls_back_to_pickle():
    ensemble = TimeSeriesEnsembleSubclass()

    header, frames = serialize(ensemble, on_error="raise")
    restored = deserialize(header, frames)

    assert header["serializer"] == "pickle"
    if _uses_legacy_nested_member_pickle(ensemble):
        assert len(frames) == 1
    assert type(restored) is TimeSeriesEnsembleSubclass


def test_unknown_wire_version_is_rejected():
    ensemble = _make_ensemble(TimeSeriesEnsemble, member_count=1, npts=64)
    header, frames = serialize(ensemble, on_error="raise")
    header["sub-header"]["version"] = 2

    with pytest.raises(ValueError, match="wire-format version=2"):
        deserialize(header, frames)


def _worker_serializer_details():
    ensemble = _make_ensemble(TimeSeriesEnsemble, member_count=1, npts=64)
    header, _ = serialize(ensemble, on_error="raise")
    return os.getpid(), header["serializer"]


def _worker_ensemble_list():
    return [
        _make_ensemble(TimeSeriesEnsemble, member_count=2, npts=128),
        _make_ensemble(SeismogramEnsemble, member_count=2, npts=128),
    ]


def _inspect_scattered_ensemble(ensemble):
    return (
        type(ensemble).__name__,
        len(ensemble.member),
        ensemble["ensemble_marker"],
        ensemble.member[-1]["member_index"],
    )


def test_raw_process_client_registers_on_driver_and_worker():
    with dask.config.set({"distributed.worker.multiprocessing-method": "spawn"}):
        with LocalCluster(
            n_workers=1,
            threads_per_worker=1,
            processes=True,
            dashboard_address=None,
        ) as cluster:
            with Client(cluster) as client:
                worker_pid, serializer = client.submit(
                    _worker_serializer_details
                ).result(timeout=15)
                assert worker_pid != os.getpid()
                assert serializer == "dask"

                expected = _worker_ensemble_list()
                restored = client.submit(_worker_ensemble_list).result(timeout=15)
                for actual, expected_ensemble in zip(restored, expected):
                    _assert_ensemble_equal(
                        actual, expected_ensemble, compare_process_ids=False
                    )

                driver_ensemble = _make_ensemble(
                    SeismogramEnsemble, member_count=2, npts=128
                )
                scattered = client.scatter(driver_ensemble)
                assert client.submit(_inspect_scattered_ensemble, scattered).result(
                    timeout=15
                ) == (
                    "SeismogramEnsemble",
                    2,
                    "SeismogramEnsemble",
                    1,
                )


def test_mspass_import_without_distributed():
    result = _run_repository_subprocess("""
import sys

for module_name in ("dask", "dask.distributed", "distributed"):
    sys.modules[module_name] = None

import mspasspy
import mspasspy.client as client_module

assert client_module.DaskClient is None
assert client_module._mspasspy_has_dask_distributed is False
client_module.DBClient.server_info = lambda self: {}
client_module.Database = lambda *args, **kwargs: object()
client_module.GlobalHistoryManager.__init__ = lambda self, *args, **kwargs: None
client = client_module.Client(scheduler="none")
assert client._scheduler_disabled is True
assert client.get_scheduler() is None
client.get_database_client().close()
""")
    assert result.returncode == 0, result.stderr
