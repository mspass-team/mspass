import os
import subprocess
from importlib.metadata import distribution, version
from pathlib import Path
from types import MethodType

import pytest
from bson import ObjectId

import mspasspy.ccore.seismic as seismic_binding
import mspasspy.db.database as database_module
import mspasspy.db.normalize as normalize_module
from mspasspy.ccore.seismic import (
    Seismogram,
    SeismogramEnsemble,
    TimeSeries,
    TimeSeriesEnsemble,
)
from mspasspy.ccore.utility import ErrorLogger, Metadata
from mspasspy.db.database import Database


class FakeCursor(list):
    pass


class _CollectionDefinition:
    def __init__(self, atomic_type):
        self.atomic_type = atomic_type

    def data_type(self):
        return self.atomic_type


class _DatabaseSchema:
    def __init__(self, collection, atomic_type):
        self.collection = collection
        self.definition = _CollectionDefinition(atomic_type)

    def default_name(self, name):
        if name == "history_object":
            return name
        return self.collection

    def __getitem__(self, name):
        assert name == self.collection
        return self.definition


class _MetadataDefinition:
    def __init__(self, collection):
        self._collection = collection

    def collection(self, key):
        assert key == "_id"
        return self._collection


class _MetadataSchema:
    def __init__(self, collection, atomic_type):
        self.atomic_type_name = atomic_type.__name__
        self.definition = _MetadataDefinition(collection)

    def __getitem__(self, name):
        assert name == self.atomic_type_name
        return self.definition


class _Undertaker:
    def handle_abortion(self, abortion):
        raise AssertionError("The contract fixture did not define any abortions")


class _ReadHarness:
    def __init__(self, ensemble, atomic_type, collection):
        self.ensemble = ensemble
        self.database_schema = _DatabaseSchema(collection, atomic_type)
        self.metadata_schema = _MetadataSchema(collection, atomic_type)
        self.stedronsky = _Undertaker()
        self.history_calls = []

    def __getitem__(self, collection):
        assert collection == self.database_schema.collection
        return object()

    def _construct_ensemble(self, *args, **kwargs):
        return self.ensemble

    def _load_history(
        self,
        datum,
        history_id,
        alg_name,
        alg_id,
        define_as_raw,
    ):
        self.history_calls.append((datum, history_id, alg_name, alg_id, define_as_raw))
        datum["history_loaded"] = True


@pytest.fixture
def ensemble_read_environment(monkeypatch):
    monkeypatch.setattr(database_module.pymongo.cursor, "Cursor", FakeCursor)

    parse_calls = []

    def fake_parse_normlist(matchers, database):
        parsed = list(matchers)
        parse_calls.append((parsed, database))
        return parsed

    monkeypatch.setattr(database_module, "parse_normlist", fake_parse_normlist)

    def fake_doclist2mdlist(
        doclist,
        database_schema,
        metadata_schema,
        collection,
        exclude_keys,
        mode,
    ):
        return [
            [Metadata(document) for document in doclist],
            bool(doclist),
            ErrorLogger(),
            [],
        ]

    monkeypatch.setattr(database_module, "doclist2mdlist", fake_doclist2mdlist)

    atomic_matcher = object()
    ensemble_matcher = object()
    normalize_calls = []

    def fake_normalize(datum, matcher):
        normalize_calls.append((datum, matcher))
        if matcher is ensemble_matcher:
            datum["ensemble_normalized"] = True
        else:
            assert matcher is atomic_matcher
            datum["atomic_normalized"] = True
            if (
                datum.is_defined("kill_during_normalize")
                and datum["kill_during_normalize"]
            ):
                datum.kill()
        return datum

    monkeypatch.setattr(normalize_module, "normalize", fake_normalize)
    return atomic_matcher, ensemble_matcher, normalize_calls, parse_calls


def _make_member(atomic_type, index, kill_during_normalize=False):
    datum = atomic_type(1)
    datum.set_live()
    datum["_id"] = ObjectId()
    datum["member_index"] = index
    datum["history_object_id"] = ObjectId()
    datum["kill_during_normalize"] = kill_during_normalize
    return datum


def _make_harness(atomic_type, ensemble_type, kill_members=()):
    input_members = [
        _make_member(atomic_type, index, index in kill_members) for index in range(2)
    ]
    ensemble = ensemble_type()
    for member in input_members:
        ensemble.member.append(member)
    ensemble.set_live()
    members = list(ensemble.member)
    collection = "wf_" + atomic_type.__name__
    return _ReadHarness(ensemble, atomic_type, collection), members


def _read_ensemble(
    harness,
    member_matchers=None,
    ensemble_matchers=None,
    load_history=False,
):
    cursor = FakeCursor([{"row": 0}, {"row": 1}])
    return Database.read_data(
        harness,
        cursor,
        collection=harness.database_schema.collection,
        normalize=member_matchers,
        normalize_ensemble=ensemble_matchers,
        load_history=load_history,
        alg_name="contract-reader",
        alg_id="issue-811",
        define_as_raw=True,
    )


def _assert_module_from_selected_build(module, relative_path):
    source_root = os.environ.get("MSPASS_TEST_SOURCE_ROOT")
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
    assert Path(module.__file__).resolve() == Path(expected_module).resolve()


def test_contract_suite_uses_selected_build_and_real_binding():
    _assert_module_from_selected_build(database_module, "mspasspy/db/database.py")
    assert Path(seismic_binding.__file__).suffix == ".so"


@pytest.mark.parametrize(
    "atomic_type,ensemble_type",
    (
        (TimeSeries, TimeSeriesEnsemble),
        (Seismogram, SeismogramEnsemble),
    ),
)
@pytest.mark.parametrize("load_history", (False, True))
@pytest.mark.parametrize("normalizer_mode", ("none", "atomic", "ensemble", "both"))
def test_normalizer_and_history_matrix(
    ensemble_read_environment,
    atomic_type,
    ensemble_type,
    load_history,
    normalizer_mode,
):
    atomic_matcher, ensemble_matcher, normalize_calls, parse_calls = (
        ensemble_read_environment
    )
    harness, members = _make_harness(atomic_type, ensemble_type)
    member_matchers = (
        [atomic_matcher] if normalizer_mode in ("atomic", "both") else None
    )
    ensemble_matchers = (
        [ensemble_matcher] if normalizer_mode in ("ensemble", "both") else None
    )

    result = _read_ensemble(
        harness,
        member_matchers=member_matchers,
        ensemble_matchers=ensemble_matchers,
        load_history=load_history,
    )

    assert result is harness.ensemble
    assert result.live
    expected_normalize_calls = []
    if ensemble_matchers:
        expected_normalize_calls.append((result, ensemble_matcher))
        assert result["ensemble_normalized"]
    if member_matchers:
        expected_normalize_calls.extend((member, atomic_matcher) for member in members)
        assert all(member["atomic_normalized"] for member in members)
    assert normalize_calls == expected_normalize_calls
    expected_parse_calls = []
    if member_matchers:
        expected_parse_calls.append(([atomic_matcher], harness))
    if ensemble_matchers:
        expected_parse_calls.append(([ensemble_matcher], harness))
    assert parse_calls == expected_parse_calls

    if load_history:
        assert harness.history_calls == [
            (
                member,
                member["history_object_id"],
                "contract-reader",
                "issue-811",
                True,
            )
            for member in members
        ]
        assert all(member["history_loaded"] for member in members)
    else:
        assert harness.history_calls == []
        assert all(not member.is_defined("history_loaded") for member in members)
    assert all(member.live for member in members)


@pytest.mark.parametrize(
    "atomic_type,ensemble_type",
    (
        (TimeSeries, TimeSeriesEnsemble),
        (Seismogram, SeismogramEnsemble),
    ),
)
def test_empty_ensemble_skips_normalization_and_history(
    ensemble_read_environment, atomic_type, ensemble_type
):
    atomic_matcher, ensemble_matcher, normalize_calls, _ = ensemble_read_environment
    harness, _ = _make_harness(atomic_type, ensemble_type)

    result = Database.read_data(
        harness,
        FakeCursor(),
        collection=harness.database_schema.collection,
        normalize=[atomic_matcher],
        normalize_ensemble=[ensemble_matcher],
        load_history=True,
    )

    assert isinstance(result, ensemble_type)
    assert result.dead()
    assert len(result.member) == 0
    assert normalize_calls == []
    assert harness.history_calls == []


@pytest.mark.parametrize(
    "atomic_type,ensemble_type",
    (
        (TimeSeries, TimeSeriesEnsemble),
        (Seismogram, SeismogramEnsemble),
    ),
)
def test_load_history_initializes_each_real_member_history(
    ensemble_read_environment, atomic_type, ensemble_type
):
    harness, members = _make_harness(atomic_type, ensemble_type)
    for member in members:
        member.erase("history_object_id")
    harness._load_history = MethodType(Database._load_history, harness)

    result = _read_ensemble(harness, load_history=True)

    assert result.live
    for member in members:
        assert member.live
        assert member.is_origin()
        node = member.current_nodedata()
        assert node.algorithm == "contract-reader"
        assert node.algid == "issue-811"


@pytest.mark.parametrize(
    "atomic_type,ensemble_type",
    (
        (TimeSeries, TimeSeriesEnsemble),
        (Seismogram, SeismogramEnsemble),
    ),
)
@pytest.mark.parametrize("kill_members", ((0,), (0, 1)))
def test_history_targets_only_members_left_live_by_normalization(
    ensemble_read_environment,
    atomic_type,
    ensemble_type,
    kill_members,
):
    atomic_matcher, _, _, _ = ensemble_read_environment
    harness, members = _make_harness(
        atomic_type, ensemble_type, kill_members=kill_members
    )

    result = _read_ensemble(
        harness,
        member_matchers=[atomic_matcher],
        load_history=True,
    )

    live_members = [member for member in members if member.live]
    assert [call[0] for call in harness.history_calls] == live_members
    assert [call[1] for call in harness.history_calls] == [
        member["history_object_id"] for member in live_members
    ]
    for index, member in enumerate(members):
        assert member.live == (index not in kill_members)
        if index in kill_members:
            assert member["is_abortion"]
        else:
            assert not member.is_defined("is_abortion")
        assert member.is_defined("history_loaded") == (index not in kill_members)
    assert result.live == bool(live_members)


@pytest.mark.parametrize(
    "atomic_type,ensemble_type",
    (
        (TimeSeries, TimeSeriesEnsemble),
        (Seismogram, SeismogramEnsemble),
    ),
)
def test_every_configured_normalizer_and_member_history_is_applied(
    ensemble_read_environment,
    monkeypatch,
    atomic_type,
    ensemble_type,
):
    _, _, normalize_calls, _ = ensemble_read_environment
    harness, members = _make_harness(atomic_type, ensemble_type)
    atomic_matchers = [object(), object()]
    ensemble_matchers = [object(), object()]

    def recording_normalize(datum, matcher):
        normalize_calls.append((datum, matcher))
        return datum

    monkeypatch.setattr(normalize_module, "normalize", recording_normalize)

    result = _read_ensemble(
        harness,
        member_matchers=atomic_matchers,
        ensemble_matchers=ensemble_matchers,
        load_history=True,
    )

    assert result is harness.ensemble
    assert normalize_calls == [
        (result, ensemble_matchers[0]),
        (result, ensemble_matchers[1]),
        (members[0], atomic_matchers[0]),
        (members[0], atomic_matchers[1]),
        (members[1], atomic_matchers[0]),
        (members[1], atomic_matchers[1]),
    ]
    assert [call[0] for call in harness.history_calls] == members


@pytest.mark.parametrize("failure_stage", ("ensemble", "member", "history"))
def test_normalizer_and_history_exceptions_are_not_silently_ignored(
    ensemble_read_environment,
    monkeypatch,
    failure_stage,
):
    atomic_matcher, ensemble_matcher, normalize_calls, _ = ensemble_read_environment
    harness, members = _make_harness(TimeSeries, TimeSeriesEnsemble)

    if failure_stage in ("ensemble", "member"):
        failing_matcher = (
            ensemble_matcher if failure_stage == "ensemble" else atomic_matcher
        )

        def failing_normalize(datum, matcher):
            normalize_calls.append((datum, matcher))
            if matcher is failing_matcher:
                raise RuntimeError(f"{failure_stage} normalization failed")
            return datum

        monkeypatch.setattr(normalize_module, "normalize", failing_normalize)
    else:

        def failing_load_history(
            datum,
            history_id,
            alg_name,
            alg_id,
            define_as_raw,
        ):
            harness.history_calls.append(
                (datum, history_id, alg_name, alg_id, define_as_raw)
            )
            raise RuntimeError("history loading failed")

        harness._load_history = failing_load_history

    with pytest.raises(RuntimeError, match=failure_stage):
        _read_ensemble(
            harness,
            member_matchers=[atomic_matcher],
            ensemble_matchers=[ensemble_matcher],
            load_history=True,
        )

    if failure_stage == "ensemble":
        assert normalize_calls == [(harness.ensemble, ensemble_matcher)]
        assert harness.history_calls == []
    elif failure_stage == "member":
        assert normalize_calls == [
            (harness.ensemble, ensemble_matcher),
            (members[0], atomic_matcher),
        ]
        assert harness.history_calls == []
    else:
        assert normalize_calls == [
            (harness.ensemble, ensemble_matcher),
            (members[0], atomic_matcher),
            (members[1], atomic_matcher),
        ]
        assert [call[0] for call in harness.history_calls] == [members[0]]
