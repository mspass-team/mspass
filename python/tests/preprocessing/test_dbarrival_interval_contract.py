from copy import deepcopy

import pytest
from obspy import UTCDateTime

from mspasspy.ccore.utility import MsPASSError
from mspasspy.preprocessing.css30.dbarrival import (
    set_arrival_by_time_interval,
    set_netcode_time_interval,
)


def _matches(document, query):
    for key, expected in query.items():
        if key not in document:
            return False
        actual = document[key]
        if isinstance(expected, dict):
            if "$gte" in expected and actual < expected["$gte"]:
                return False
            if "$lte" in expected and actual > expected["$lte"]:
                return False
        elif actual != expected:
            return False
    return True


class _Cursor(list):
    def sort(self, key, direction):
        assert direction == 1
        list.sort(self, key=lambda document: document[key])
        return self

    def rewind(self):
        return self


class _Collection:
    def __init__(self, documents):
        self.documents = deepcopy(documents)
        self.find_calls = []
        self.count_calls = []
        self.update_calls = []

    def find(self, query, **kwargs):
        self.find_calls.append((deepcopy(query), deepcopy(kwargs)))
        return _Cursor(
            deepcopy(document)
            for document in self.documents
            if _matches(document, query)
        )

    def count_documents(self, query):
        self.count_calls.append(deepcopy(query))
        return sum(_matches(document, query) for document in self.documents)

    def update_one(self, query, update):
        self.update_calls.append((deepcopy(query), deepcopy(update)))
        for document in self.documents:
            if _matches(document, query):
                document.update(update["$set"])
                return
        raise AssertionError(f"update query matched no document: {query}")


class _Database:
    def __init__(self, arrivals, sites=()):
        self.arrival = _Collection(arrivals)
        self.site = _Collection(sites)

    def __getitem__(self, collection):
        assert collection == "arrival"
        return self.arrival


class _NoDatabaseAccess:
    def __getitem__(self, collection):
        raise AssertionError(f"unexpected collection access: {collection}")

    @property
    def arrival(self):
        raise AssertionError("unexpected arrival access")

    @property
    def site(self):
        raise AssertionError("unexpected site access")


@pytest.mark.parametrize("use_immortal_cursor", [False, True])
def test_site_intervals_update_only_matching_arrivals(use_immortal_cursor):
    sites = [
        {"_id": "site-a", "sta": "AAA", "net": "A", "starttime": 0.0, "endtime": 10.0},
        {"_id": "site-b", "sta": "AAA", "net": "B", "starttime": 20.0, "endtime": 30.0},
    ]
    arrivals = [
        {"_id": "a0", "sta": "AAA", "time": 0.0},
        {"_id": "a10", "sta": "AAA", "time": 10.0},
        {"_id": "gap", "sta": "AAA", "time": 15.0},
        {"_id": "b20", "sta": "AAA", "time": 20.0},
        {"_id": "b30", "sta": "AAA", "time": 30.0},
        {"_id": "other", "sta": "BBB", "time": 5.0},
    ]
    database = _Database(arrivals, sites)

    count = set_arrival_by_time_interval(
        database,
        sta="AAA",
        allowed_overlap=0.0,
        use_immortal_cursor=use_immortal_cursor,
    )

    assert type(count) is int
    assert count == 4
    expected_intervals = [
        {"sta": "AAA", "time": {"$gte": 0.0, "$lte": 10.0}},
        {"sta": "AAA", "time": {"$gte": 20.0, "$lte": 30.0}},
    ]
    cursor_options = {"no_cursor_timeout": True} if use_immortal_cursor else {}
    assert database.site.find_calls == [({"sta": "AAA"}, cursor_options)]
    assert database.arrival.find_calls == [
        (expected_intervals[0], cursor_options),
        (expected_intervals[1], cursor_options),
    ]
    assert database.arrival.update_calls == [
        ({**expected_intervals[0], "_id": "a0"}, {"$set": {"net": "A"}}),
        ({**expected_intervals[0], "_id": "a10"}, {"$set": {"net": "A"}}),
        ({**expected_intervals[1], "_id": "b20"}, {"$set": {"net": "B"}}),
        ({**expected_intervals[1], "_id": "b30"}, {"$set": {"net": "B"}}),
    ]
    assert {
        document["_id"]: document.get("net") for document in database.arrival.documents
    } == {
        "a0": "A",
        "a10": "A",
        "gap": None,
        "b20": "B",
        "b30": "B",
        "other": None,
    }


@pytest.mark.parametrize("use_immortal_cursor", [False, True])
def test_forced_interval_updates_share_the_read_predicate(use_immortal_cursor):
    arrivals = [
        {"_id": "left", "sta": "AAA", "time": 0.0},
        {"_id": "right", "sta": "AAA", "time": 10.0},
        {"_id": "outside", "sta": "AAA", "time": 11.0},
        {"_id": "other", "sta": "BBB", "time": 5.0},
    ]
    database = _Database(arrivals)

    count = set_netcode_time_interval(
        database,
        sta="AAA",
        net="XX",
        starttime=UTCDateTime(0.0),
        endtime=UTCDateTime(10.0),
        use_immortal_cursor=use_immortal_cursor,
    )

    assert type(count) is int
    assert count == 2
    interval = {"sta": "AAA", "time": {"$gte": 0.0, "$lte": 10.0}}
    cursor_options = {"no_cursor_timeout": True} if use_immortal_cursor else {}
    assert database.arrival.count_calls == [interval]
    assert database.arrival.find_calls == [(interval, cursor_options)]
    assert database.arrival.update_calls == [
        ({**interval, "_id": "left"}, {"$set": {"net": "XX"}}),
        ({**interval, "_id": "right"}, {"$set": {"net": "XX"}}),
    ]
    assert {
        document["_id"]: document.get("net") for document in database.arrival.documents
    } == {
        "left": "XX",
        "right": "XX",
        "outside": None,
        "other": None,
    }


@pytest.mark.parametrize("use_immortal_cursor", [False, True])
def test_forced_interval_empty_result_returns_zero(use_immortal_cursor):
    database = _Database([])

    count = set_netcode_time_interval(
        database,
        sta="AAA",
        net="XX",
        use_immortal_cursor=use_immortal_cursor,
    )

    assert type(count) is int
    assert count == 0
    query = {"sta": "AAA"}
    cursor_options = {"no_cursor_timeout": True} if use_immortal_cursor else {}
    assert database.arrival.count_calls == [query]
    assert database.arrival.find_calls == [(query, cursor_options)]
    assert database.arrival.update_calls == []


@pytest.mark.parametrize("use_immortal_cursor", [False, True])
def test_site_interval_empty_result_returns_zero(use_immortal_cursor):
    sites = [
        {"_id": "site", "sta": "AAA", "net": "A", "starttime": 0.0, "endtime": 10.0}
    ]
    database = _Database([], sites)

    count = set_arrival_by_time_interval(
        database,
        sta="AAA",
        allowed_overlap=0.0,
        use_immortal_cursor=use_immortal_cursor,
    )

    assert type(count) is int
    assert count == 0
    interval = {"sta": "AAA", "time": {"$gte": 0.0, "$lte": 10.0}}
    cursor_options = {"no_cursor_timeout": True} if use_immortal_cursor else {}
    assert database.site.find_calls == [({"sta": "AAA"}, cursor_options)]
    assert database.arrival.find_calls == [(interval, cursor_options)]
    assert database.arrival.update_calls == []


@pytest.mark.parametrize(
    "function, kwargs",
    [
        (set_arrival_by_time_interval, {}),
        (set_arrival_by_time_interval, {"sta": ""}),
        (set_arrival_by_time_interval, {"sta": 3}),
        (set_netcode_time_interval, {"sta": None, "net": "XX"}),
        (set_netcode_time_interval, {"sta": "", "net": "XX"}),
        (set_netcode_time_interval, {"sta": 3, "net": "XX"}),
        (set_netcode_time_interval, {"sta": "AAA", "net": None}),
        (set_netcode_time_interval, {"sta": "AAA", "net": ""}),
        (set_netcode_time_interval, {"sta": "AAA", "net": 3}),
    ],
)
def test_required_station_and_network_fail_before_database_access(function, kwargs):
    with pytest.raises(ValueError):
        function(_NoDatabaseAccess(), **kwargs)


@pytest.mark.parametrize("bad_net", [None, "", 7])
def test_site_interval_rejects_invalid_network_before_arrival_access(bad_net):
    database = _Database(
        [{"_id": "arrival", "sta": "AAA", "time": 5.0}],
        [
            {
                "_id": "site",
                "sta": "AAA",
                "net": bad_net,
                "starttime": 0.0,
                "endtime": 10.0,
            }
        ],
    )
    arrivals_before = deepcopy(database.arrival.documents)

    with pytest.raises(ValueError, match="site net must be a nonempty string"):
        set_arrival_by_time_interval(database, sta="AAA")

    assert database.site.find_calls == [({"sta": "AAA"}, {})]
    assert database.arrival.find_calls == []
    assert database.arrival.update_calls == []
    assert database.arrival.documents == arrivals_before


@pytest.mark.parametrize("use_immortal_cursor", [False, True])
def test_overlapping_site_intervals_fail_before_arrival_access(use_immortal_cursor):
    sites = [
        {"_id": "site-a", "sta": "AAA", "net": "A", "starttime": 0.0, "endtime": 10.0},
        {"_id": "site-b", "sta": "AAA", "net": "B", "starttime": 5.0, "endtime": 15.0},
    ]
    arrivals = [{"_id": "arrival", "sta": "AAA", "time": 7.0}]
    database = _Database(arrivals, sites)
    arrivals_before = deepcopy(database.arrival.documents)

    with pytest.raises(MsPASSError, match="Overlapping time intervals"):
        set_arrival_by_time_interval(
            database,
            sta="AAA",
            allowed_overlap=0.0,
            use_immortal_cursor=use_immortal_cursor,
        )

    cursor_options = {"no_cursor_timeout": True} if use_immortal_cursor else {}
    assert database.site.find_calls == [({"sta": "AAA"}, cursor_options)]
    assert database.arrival.find_calls == []
    assert database.arrival.count_calls == []
    assert database.arrival.update_calls == []
    assert database.arrival.documents == arrivals_before


@pytest.mark.parametrize(
    "starttime,endtime",
    [
        (UTCDateTime(0.0), None),
        (None, UTCDateTime(10.0)),
        (0.0, UTCDateTime(10.0)),
        (UTCDateTime(0.0), 10.0),
    ],
)
def test_invalid_forced_interval_fails_before_query_or_write(starttime, endtime):
    database = _Database([{"_id": "arrival", "sta": "AAA", "time": 5.0}])
    arrivals_before = deepcopy(database.arrival.documents)

    with pytest.raises(MsPASSError):
        set_netcode_time_interval(
            database,
            sta="AAA",
            net="XX",
            starttime=starttime,
            endtime=endtime,
        )

    assert database.arrival.count_calls == []
    assert database.arrival.find_calls == []
    assert database.arrival.update_calls == []
    assert database.arrival.documents == arrivals_before
