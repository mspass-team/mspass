import copy
import os
import subprocess
from importlib.metadata import distribution, version
from pathlib import Path

import numpy as np
from obspy import read, Stream, Trace, UTCDateTime
import pytest

import mspasspy.preprocessing.seed.ensembles as seed_ensembles
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError


def _assert_expected_module_loaded():
    source_root = os.environ.get("MSPASS_TEST_SOURCE_ROOT")
    relative_path = Path("mspasspy/preprocessing/seed/ensembles.py")
    if source_root:
        expected_module = Path(source_root) / relative_path
    else:
        expected_module = distribution("mspasspy").locate_file(relative_path)
        installed_version = version("mspasspy")
        installed_commit = installed_version.partition("+g")[2].partition(".")[0]
        assert installed_commit, "installed mspasspy version lacks a source commit"
        repository_root = next(
            parent
            for parent in Path(__file__).resolve().parents
            if (parent / ".git").exists()
        )
        checkout_commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repository_root, text=True
        ).strip()
        assert checkout_commit.startswith(installed_commit)
    assert Path(seed_ensembles.__file__).resolve() == Path(expected_module).resolve()


def _stream():
    traces = []
    for i in range(3):
        header = {
            "network": f"N{i}",
            "station": f"S{i}",
            "channel": f"BH{i}",
            "location": f"0{i}",
            "starttime": UTCDateTime(1700000000.0 + i),
            "sampling_rate": 20.0 + i,
            "calib": 2.0 + i,
        }
        traces.append(
            Trace(data=np.array([10.0 * i + j for j in range(4)]), header=header)
        )
    return Stream(traces)


SEED_IDS = [
    "11111111-1111-4111-8111-111111111111",
    "22222222-2222-4222-8222-222222222222",
    "33333333-3333-4333-8333-333333333333",
]


def _document(ids=SEED_IDS):
    return {
        "format": "mseed",
        "mover": "obspy_seed_ensemble_reader",
        "dir": "/not-read-from-disk",
        "dfile": "three-traces.mseed",
        "ensemble_tag": "contract-test",
        "members": [{"seed_file_id": seed_id} for seed_id in ids],
    }


def _stream_snapshot(stream):
    return [(dict(trace.stats), trace.data.copy()) for trace in stream]


def _assert_stream_unchanged(stream, snapshot):
    for trace, (stats, data) in zip(stream, snapshot):
        assert dict(trace.stats) == stats
        np.testing.assert_array_equal(trace.data, data)


def test_load_one_ensemble_history_ids_are_member_aligned():
    _assert_expected_module_loaded()
    mseed_file = Path(__file__).parents[1] / "data" / "3channels.mseed"
    stream = read(str(mseed_file), format="mseed")
    doc = _document()
    doc["dir"] = str(mseed_file.parent)
    doc["dfile"] = mseed_file.name
    original_doc = copy.deepcopy(doc)

    result = seed_ensembles.load_one_ensemble(
        doc,
        create_history=True,
        jobname="history-job",
        jobid="history-job-id",
        algid="reader-version",
    )

    assert result is not None
    assert len(result.member) == 3
    assert [member.id() for member in result.member] == SEED_IDS
    for i, member in enumerate(result.member):
        assert member.is_raw()
        assert member.jobname() == "history-job"
        assert member.jobid() == "history-job-id"
        assert member["sta"] == stream[i].stats.station
        assert member["chan"] == stream[i].stats.channel
        np.testing.assert_array_equal(member.data, stream[i].data)
    assert doc == original_doc


class _MembersAccessGuard(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.members_reads = 0

    def __getitem__(self, key):
        if key == "members":
            self.members_reads += 1
            raise AssertionError("history-off path accessed doc['members']")
        return super().__getitem__(key)


def test_load_one_ensemble_history_off_never_reads_members(monkeypatch):
    _assert_expected_module_loaded()
    stream = _stream()
    guarded_doc = _MembersAccessGuard(_document())
    monkeypatch.setattr(seed_ensembles, "read", lambda *args, **kwargs: stream)

    result = seed_ensembles.load_one_ensemble(guarded_doc, create_history=False)

    assert guarded_doc.members_reads == 0
    assert len(result.member) == 3
    assert result["ensemble_tag"] == "contract-test"
    for i, member in enumerate(result.member):
        assert member.is_empty()
        assert member["net"] == f"N{i}"
        assert member["sta"] == f"S{i}"
        assert member["chan"] == f"BH{i}"
        assert member["loc"] == f"0{i}"
        assert member.t0 == stream[i].stats.starttime.timestamp
        assert member.dt == pytest.approx(stream[i].stats.delta)
        np.testing.assert_array_equal(member.data, stream[i].data)


def _invalid_document(case):
    doc = _document()
    if case == "missing_members":
        del doc["members"]
    elif case == "short_members":
        doc["members"] = doc["members"][:-1]
    elif case == "long_members":
        doc["members"].append({"seed_file_id": "extra-id"})
    elif case == "missing_middle_id":
        doc["members"][1] = {}
    elif case == "none_middle_id":
        doc["members"][1]["seed_file_id"] = None
    elif case == "empty_middle_id":
        doc["members"][1]["seed_file_id"] = ""
    elif case == "nonstring_middle_id":
        doc["members"][1]["seed_file_id"] = 42
    else:
        raise AssertionError(f"unhandled test case {case}")
    return doc


@pytest.mark.parametrize(
    "case, expected_message",
    [
        (
            "missing_members",
            "load_one_ensemble:  create_history=True requires doc['members']",
        ),
        (
            "short_members",
            "load_one_ensemble:  input member count mismatch; "
            "len(doc['members'])=2 but len(stream)=3",
        ),
        (
            "long_members",
            "load_one_ensemble:  input member count mismatch; "
            "len(doc['members'])=4 but len(stream)=3",
        ),
        (
            "missing_middle_id",
            "load_one_ensemble:  input member 1 is missing seed_file_id",
        ),
        (
            "none_middle_id",
            "load_one_ensemble:  input member 1 has an invalid seed_file_id; "
            "expected a nonempty string",
        ),
        (
            "empty_middle_id",
            "load_one_ensemble:  input member 1 has an invalid seed_file_id; "
            "expected a nonempty string",
        ),
        (
            "nonstring_middle_id",
            "load_one_ensemble:  input member 1 has an invalid seed_file_id; "
            "expected a nonempty string",
        ),
    ],
)
def test_load_one_ensemble_rejects_invalid_history_before_output(
    case, expected_message, monkeypatch, capsys
):
    _assert_expected_module_loaded()
    stream = _stream()
    stream_before = _stream_snapshot(stream)
    doc = _invalid_document(case)
    doc_before = copy.deepcopy(doc)
    output_construction = []

    monkeypatch.setattr(seed_ensembles, "read", lambda *args, **kwargs: stream)

    def record_output_construction(*args, **kwargs):
        output_construction.append((args, kwargs))
        raise AssertionError("output construction began before history validation")

    monkeypatch.setattr(seed_ensembles, "Metadata", record_output_construction)
    monkeypatch.setattr(seed_ensembles, "ProcessingHistory", record_output_construction)
    monkeypatch.setattr(
        seed_ensembles, "TimeSeriesEnsemble", record_output_construction
    )
    monkeypatch.setattr(seed_ensembles, "Trace2TimeSeries", record_output_construction)
    capsys.readouterr()

    with pytest.raises(MsPASSError) as excinfo:
        seed_ensembles.load_one_ensemble(doc, create_history=True, verbose=True)

    assert excinfo.value.severity == ErrorSeverity.Invalid
    assert excinfo.value.message == expected_message
    assert output_construction == []
    assert doc == doc_before
    _assert_stream_unchanged(stream, stream_before)
    captured = capsys.readouterr()
    assert captured.out == ""
