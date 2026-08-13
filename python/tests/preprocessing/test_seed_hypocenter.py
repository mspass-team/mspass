import copy

import pytest

from mspasspy.ccore.seismic import TimeSeries, TimeSeriesEnsemble
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError
from mspasspy.preprocessing.seed.ensembles import (
    load_channel_data,
    load_hypocenter_data_by_time,
    load_site_data,
)


def _matches(document, query):
    for key, condition in query.items():
        if key not in document:
            return False
        value = document[key]
        if not isinstance(condition, dict):
            if value != condition:
                return False
            continue
        for operator, expected in condition.items():
            if operator == "$eq" and value != expected:
                return False
            if operator == "$gte" and value < expected:
                return False
            if operator == "$lte" and value > expected:
                return False
            if operator == "$gt" and value <= expected:
                return False
            if operator == "$lt" and value >= expected:
                return False
    return True


class FakeCollection:
    def __init__(self, documents=()):
        self.documents = copy.deepcopy(list(documents))
        self.queries = []

    def find(self, query=None):
        query = {} if query is None else copy.deepcopy(query)
        self.queries.append(query)
        return iter(
            copy.deepcopy(
                [document for document in self.documents if _matches(document, query)]
            )
        )

    def count_documents(self, query):
        return sum(_matches(document, query) for document in self.documents)

    def find_one(self, query):
        return next(self.find(query), None)


class FakeDatabase:
    def __init__(self, sources=(), arrivals=(), sites=(), channels=()):
        self.source = FakeCollection(sources)
        self.arrival = FakeCollection(arrivals)
        self.site = FakeCollection(sites)
        self.channel = FakeCollection(channels)


def _source(event_id, origin_time, **overrides):
    document = {
        "_id": f"source-{event_id}",
        "evid": event_id,
        "lat": 10.0 + float(origin_time),
        "lon": 20.0 + float(origin_time),
        "depth": 5.0,
        "time": origin_time,
    }
    document.update(overrides)
    return document


def _arrival(event_id, net, sta, arrival_time, phase="P"):
    return {
        "evid": event_id,
        "net": net,
        "sta": sta,
        "phase": phase,
        "time": arrival_time,
    }


def _ensemble(member_specs):
    ensemble = TimeSeriesEnsemble(len(member_specs))
    for spec in member_specs:
        datum = TimeSeries(1)
        datum.t0 = spec[2]
        datum["net"] = spec[0]
        datum["sta"] = spec[1]
        datum["marker"] = f"{spec[0]}.{spec[1]}"
        datum.set_live()
        if len(spec) == 4 and not spec[3]:
            datum.kill()
        ensemble.member.append(datum)
    ensemble["ensemble_marker"] = "unchanged"
    ensemble.set_live()
    return ensemble


def _assert_source_metadata(datum, source):
    assert datum["source_id"] == source["_id"]
    assert datum["source_lat"] == source["lat"]
    assert datum["source_lon"] == source["lon"]
    assert datum["source_depth"] == source["depth"]
    assert datum["source_time"] == source["time"]


@pytest.mark.parametrize("kill_null", [False, True])
def test_no_candidate_event_has_defined_kill_behavior(kill_null, capsys):
    ensemble = _ensemble([("IU", "AAA", 100.0), ("IU", "BBB", 101.0)])
    database = FakeDatabase()

    count = load_hypocenter_data_by_time(database, ensemble, kill_null=kill_null)

    assert count == 0
    assert [datum.dead() for datum in ensemble.member] == [kill_null, kill_null]
    assert all("source_id" not in datum for datum in ensemble.member)
    assert ensemble["ensemble_marker"] == "unchanged"
    assert capsys.readouterr().out == ""


def test_origin_time_mode_selects_lowest_rms_before_mutation(capsys):
    source_a = _source("a", 100.0)
    source_b = _source("b", 102.0)
    database = FakeDatabase(sources=[source_b, source_a])
    ensemble = _ensemble(
        [
            ("IU", "AAA", 95.0),
            ("IU", "BBB", 96.0),
            ("IU", "CCC", 130.0),
            ("IU", "DDD", 95.0, False),
        ]
    )

    count = load_hypocenter_data_by_time(
        database,
        ensemble,
        t0_definition="origin_time",
        t0_offset=5.0,
        dt=4.0,
        kill_null=False,
    )

    assert count == 2
    _assert_source_metadata(ensemble.member[0], source_a)
    _assert_source_metadata(ensemble.member[1], source_a)
    assert ensemble.member[2].live
    assert "source_id" not in ensemble.member[2]
    assert ensemble.member[3].dead()
    assert "source_id" not in ensemble.member[3]
    assert capsys.readouterr().out == ""


def test_origin_time_tie_uses_lexical_event_id_and_dt_is_inclusive():
    lexical_later = _source("z", 100.0)
    lexical_first = _source("a", 102.0)
    database = FakeDatabase(sources=[lexical_later, lexical_first])
    ensemble = _ensemble([("IU", "AAA", 101.0)])

    count = load_hypocenter_data_by_time(database, ensemble, dt=2.0, kill_null=True)

    assert count == 1
    _assert_source_metadata(ensemble.member[0], lexical_first)

    boundary_ensemble = _ensemble([("IU", "BBB", 104.0), ("IU", "CCC", 104.1)])
    boundary_database = FakeDatabase(sources=[lexical_first])
    count = load_hypocenter_data_by_time(
        boundary_database, boundary_ensemble, dt=2.0, kill_null=True
    )
    assert count == 1
    _assert_source_metadata(boundary_ensemble.member[0], lexical_first)
    assert boundary_ensemble.member[1].dead()


def test_falsey_event_id_is_used_for_phase_association():
    source = _source(0, 10.0)
    arrival = _arrival(0, "IU", "AAA", 100.0)
    arrival["pick_time"] = arrival.pop("time")
    database = FakeDatabase(sources=[source], arrivals=[arrival])
    ensemble = _ensemble([("IU", "AAA", 100.0)])

    count = load_hypocenter_data_by_time(
        database,
        ensemble,
        dbtime_key="pick_time",
        t0_definition="phase_time",
        dt=0.0,
    )

    assert count == 1
    _assert_source_metadata(ensemble.member[0], source)


def test_custom_event_id_key_is_exact_and_missing_key_is_not_none():
    source_a = _source("unused-a", 10.0, _id="source-a")
    source_a["catalog_id"] = "a"
    source_a.pop("evid")
    source_b = _source("unused-b", 20.0, _id="source-b")
    source_b["catalog_id"] = "b"
    source_b.pop("evid")
    missing_key_source = _source(None, 30.0, _id="source-none")
    missing_key_source["catalog_id"] = None
    missing_key_source.pop("evid")
    arrival_a = _arrival("unused", "IU", "AAA", 110.0)
    arrival_a["catalog_id"] = "a"
    arrival_b = _arrival("unused", "IU", "AAA", 100.0)
    arrival_b["catalog_id"] = "b"
    missing_event_key = _arrival("unused", "IU", "AAA", 100.0)
    del missing_event_key["evid"]
    database = FakeDatabase(
        sources=[source_a, missing_key_source, source_b],
        arrivals=[missing_event_key, arrival_a, arrival_b],
    )
    ensemble = _ensemble([("IU", "AAA", 100.0)])

    count = load_hypocenter_data_by_time(
        database,
        ensemble,
        event_id_key="catalog_id",
        t0_definition="phase_time",
        dt=10.0,
        kill_null=False,
    )

    assert count == 1
    _assert_source_metadata(ensemble.member[0], source_b)


def test_all_dead_members_return_without_database_access_or_mutation():
    ensemble = _ensemble([("IU", "AAA", 100.0, False)])
    member = ensemble.member[0]
    before = (dict(member), member.live)
    database = FakeDatabase()

    count = load_hypocenter_data_by_time(database, ensemble)

    assert count == 0
    assert ensemble.member[0] is member
    assert (dict(member), member.live) == before
    assert database.source.queries == []
    assert database.arrival.queries == []


@pytest.mark.parametrize("kill_null", [False, True])
def test_phase_time_mode_requires_exact_net_sta_and_phase(kill_null, capsys):
    source_a = _source("a", 10.0)
    source_b = _source("b", 20.0)
    arrivals = [
        _arrival("a", "IU", "AAA", 100.0),
        _arrival("a", "IU", "BBB", 106.0),
        _arrival("a", "XX", "AAA", 100.0, phase="S"),
        _arrival("b", "IU", "AAA", 99.0),
        _arrival("b", "IU", "BBB", 103.0),
    ]
    database = FakeDatabase(sources=[source_b, source_a], arrivals=arrivals)
    ensemble = _ensemble(
        [
            ("IU", "AAA", 95.0),
            ("IU", "BBB", 100.0),
            ("XX", "AAA", 95.0),
            ("IU", "CCC", 100.0),
        ]
    )

    count = load_hypocenter_data_by_time(
        database,
        ensemble,
        t0_definition="phase_time",
        t0_offset=5.0,
        dt=5.0,
        phase="P",
        kill_null=kill_null,
    )

    assert count == 2
    _assert_source_metadata(ensemble.member[0], source_a)
    _assert_source_metadata(ensemble.member[1], source_a)
    for datum in ensemble.member[2:]:
        assert datum.dead() is kill_null
        assert "source_id" not in datum
    assert database.arrival.queries == [{"phase": "P"}]
    assert capsys.readouterr().out == ""


def test_nonfinite_residual_does_not_match():
    source = _source("one", 100.0)
    database = FakeDatabase(sources=[source])
    ensemble = _ensemble([("IU", "AAA", float("nan")), ("IU", "BBB", 100.0)])

    count = load_hypocenter_data_by_time(database, ensemble, kill_null=True)

    assert count == 1
    assert ensemble.member[0].dead()
    _assert_source_metadata(ensemble.member[1], source)


def test_rms_remains_finite_for_large_finite_residuals():
    worse_but_lexical_first = _source("a", 0.0)
    better_but_lexical_later = _source("z", 5.0e307)
    database = FakeDatabase(sources=[worse_but_lexical_first, better_but_lexical_later])
    ensemble = _ensemble([("IU", "AAA", 1.0e308)])

    count = load_hypocenter_data_by_time(database, ensemble, dt=1.0e308)

    assert count == 1
    _assert_source_metadata(ensemble.member[0], better_but_lexical_later)


def test_selected_source_validation_is_atomic_and_invalid(capsys):
    malformed_source = _source("bad", 100.0)
    del malformed_source["lat"]
    database = FakeDatabase(sources=[malformed_source])
    ensemble = _ensemble([("IU", "AAA", 100.0), ("IU", "BBB", 130.0)])
    ensemble.member[0]["source_id"] = "preexisting"
    before = [(dict(datum), datum.live) for datum in ensemble.member]

    with pytest.raises(MsPASSError) as excinfo:
        load_hypocenter_data_by_time(database, ensemble, dt=5.0, kill_null=True)

    assert excinfo.value.severity == ErrorSeverity.Invalid
    assert str(excinfo.value).startswith("load_hypocenter_data_by_time: ")
    assert "lat" in str(excinfo.value)
    assert [(dict(datum), datum.live) for datum in ensemble.member] == before
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    "kwargs, diagnostic",
    [
        ({"t0_definition": "unknown"}, "t0_definition"),
        ({"dt": -1.0}, "dt"),
        ({"dt": float("nan")}, "dt"),
        ({"t0_offset": float("inf")}, "t0_offset"),
    ],
)
def test_invalid_configuration_is_invalid_and_atomic(kwargs, diagnostic, capsys):
    database = FakeDatabase(sources=[_source("one", 100.0)])
    ensemble = _ensemble([("IU", "AAA", 100.0)])
    before = (dict(ensemble.member[0]), ensemble.member[0].live)

    with pytest.raises(MsPASSError) as excinfo:
        load_hypocenter_data_by_time(database, ensemble, **kwargs)

    assert excinfo.value.severity == ErrorSeverity.Invalid
    assert str(excinfo.value).startswith("load_hypocenter_data_by_time: ")
    assert diagnostic in str(excinfo.value)
    assert (dict(ensemble.member[0]), ensemble.member[0].live) == before
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("missing", ["db", "ens"])
def test_missing_required_argument_is_invalid_and_silent(missing, capsys):
    database = FakeDatabase()
    ensemble = _ensemble([("IU", "AAA", 100.0)])
    before = (dict(ensemble.member[0]), ensemble.member[0].live)
    kwargs = {"db": database, "ens": ensemble}
    kwargs[missing] = None

    with pytest.raises(MsPASSError) as excinfo:
        load_hypocenter_data_by_time(**kwargs)

    assert excinfo.value.severity == ErrorSeverity.Invalid
    assert str(excinfo.value).startswith("load_hypocenter_data_by_time: ")
    assert missing in str(excinfo.value)
    assert (dict(ensemble.member[0]), ensemble.member[0].live) == before
    assert capsys.readouterr().out == ""


def test_adjacent_site_and_channel_loaders_still_use_their_collections():
    site = {
        "site_id": "site-id",
        "net": "IU",
        "sta": "AAA",
        "starttime": 0.0,
        "endtime": 200.0,
        "lat": 1.0,
        "lon": 2.0,
        "elev": 3.0,
    }
    channel = {
        "_id": "channel-id",
        "net": "IU",
        "sta": "AAA",
        "loc": "00",
        "chan": "BHZ",
        "starttime": 0.0,
        "endtime": 200.0,
        "lat": 4.0,
        "lon": 5.0,
        "elev": 6.0,
        "vang": 0.0,
        "hang": 90.0,
    }
    database = FakeDatabase(sites=[site], channels=[channel])
    ensemble = _ensemble([("IU", "AAA", 100.0)])
    datum = ensemble.member[0]
    datum["starttime"] = 100.0
    datum["loc"] = "00"
    datum["chan"] = "BHZ"

    assert load_site_data(database, ensemble) is ensemble
    assert datum["site_id"] == "site-id"
    assert load_channel_data(database, ensemble) is ensemble
    assert datum["site_id"] == "channel-id"
    assert datum["site_lat"] == 4.0
