import os

from mspasspy.seismic.gather import (
    extractDataFromMsPassObject,
    extractDataFromOldEnsemble,
    extractMemberMetadataFromOldEnsemble,
    Gather,
    SeismogramGather,
)

from mspasspy.ccore.seismic import (
    TimeSeriesEnsemble,
    SeismogramEnsemble,
    TimeSeries,
    Seismogram,
)

from mspasspy.ccore.utility import (
    Metadata,
)

import numpy as np
import pandas as pd


def make_constant_data_ts(d, t0=0.0, dt=0.1, nsamp=5, val=1.0):
    """
    This function is stolen from test_ccore.py

    Fills TimeSeries (or _CoreTimeSeries) data vector with
    a constant value of a specified length and start time.
    Used for testing arithmetic operators.

    Parameters
    ----------
    d : TYPE
        DESCRIPTION.  TimeSeries or _CoreTimeSeries skeleton to build upon
    t0 : TYPE, optional
        DESCRIPTION. The default is 0.0. data start time
    dt : TYPE, optional
        DESCRIPTION. The default is 0.1.  sample interval
    nsamp : TYPE, optional
        DESCRIPTION. The default is 5.  length of data vector to generate

    Returns
    -------
    None.

    """
    d.npts = nsamp
    d.t0 = t0
    d.dt = dt
    d.set_live()
    for i in range(nsamp):
        d.data[i] = val
    return d


def make_constant_data_seis(d, t0=0.0, dt=0.1, nsamp=5, val=1.0):
    """
    Fills Seismogram (or Seismogram) data vector with
    a constant value of a specified length and start time.
    Used for testing arithmetic operators.

    Parameters
    ----------
    d : TYPE
        DESCRIPTION.  TimeSeries or _CoreTimeSeries skeleton to build upon
    t0 : TYPE, optional
        DESCRIPTION. The default is 0.0. data start time
    dt : TYPE, optional
        DESCRIPTION. The default is 0.1.  sample interval
    nsamp : TYPE, optional
        DESCRIPTION. The default is 5.  length of data vector to generate

    Returns
    -------
    None.

    """
    d.npts = nsamp
    d.t0 = t0
    d.dt = dt
    d.set_live()
    for i in range(nsamp):
        for k in range(3):
            d.data[k, i] = val
    return d


def test_extractDataFromMsPassObject():
    # test timeseries
    d = TimeSeries(10)
    d = make_constant_data_ts(d)
    d_data = np.full((5), 1.0)
    assert np.array_equal(extractDataFromMsPassObject(d), d_data)
    # test seismogram
    d2 = Seismogram(10)
    d2 = make_constant_data_seis(d2, t0=-0.2, nsamp=6, val=2.0)
    d2_data = np.full((3, 6), 2.0)
    assert np.array_equal(extractDataFromMsPassObject(d2), d2_data)


def test_extractDataFromOldEnsemble():
    # test timeseries ensemble
    md = Metadata()
    md["double"] = 3.14
    md["bool"] = True
    md["long"] = 7
    es = TimeSeriesEnsemble(md, 3)
    d = TimeSeries(10)
    d = make_constant_data_ts(d)
    for i in range(3):
        es.member.append(d)
    es_data = np.full((3, 1, 5), 1.0)
    assert np.array_equal(extractDataFromOldEnsemble(es), es_data)
    # test seimogram ensemble
    es2 = SeismogramEnsemble(md, 3)
    d2 = Seismogram(10)
    d2 = make_constant_data_seis(d2, t0=-0.2, nsamp=6, val=2.0)
    d2_data = np.full((3, 6), 2.0)
    for i in range(3):
        es2.member.append(d2)
    es_data2 = np.full((3, 3, 6), 2.0)
    assert np.array_equal(extractDataFromOldEnsemble(es2), es_data2)


def test_extractMemberMetadataFromOldEnsemble():
    # test timeseries ensemble
    md = Metadata()
    md["double"] = 3.14
    md["bool"] = True
    md["long"] = 7
    es = TimeSeriesEnsemble(md, 3)
    d = TimeSeries(10)
    d = make_constant_data_ts(d)
    for i in range(3):
        es.member.append(d)
    es_data = np.full((3, 1, 5), 1.0)
    es_member_metadata = pd.DataFrame(
        data=np.array([[0, 0.1, 5, True], [0, 0.1, 5, True], [0, 0.1, 5, True]]),
        columns=["starttime", "delta", "npts", "is_live"],
    )

    assert np.array_equal(
        extractMemberMetadataFromOldEnsemble(es).sort_index(axis=1),
        es_member_metadata.sort_index(axis=1),
    )
    # test seimogram ensemble
    es2 = SeismogramEnsemble(md, 3)
    d2 = Seismogram(10)
    d2 = make_constant_data_seis(d2, t0=-0.2, nsamp=6, val=2.0)
    d2_data = np.full((3, 6), 2.0)
    for i in range(3):
        es2.member.append(d2)
    es_data2 = np.full((3, 3, 6), 2.0)
    es2_member_metadata = pd.DataFrame(
        data=np.array(
            [[-0.2, 0.1, 6, True], [-0.2, 0.1, 6, True], [-0.2, 0.1, 6, True]]
        ),
        columns=["starttime", "delta", "npts", "is_live"],
    )

    assert np.array_equal(
        extractMemberMetadataFromOldEnsemble(es2).sort_index(axis=1),
        es2_member_metadata.sort_index(axis=1),
    )


class TestGather:
    def setup_class(self):
        self.md_dict = {
            "delta": [5, 5],
            "starttime": [1.0, 2.0],
            "is_live": [True, False],
            "time": [2.0, 3.0],
        }
        self.md = pd.DataFrame.from_dict(self.md_dict)
        es_md = Metadata(self.md_dict)
        es = TimeSeriesEnsemble(es_md, 3)
        d = TimeSeries(10)
        d = make_constant_data_ts(d)
        for i in range(3):
            es.member.append(d)
        self.gather = Gather(input_obj=es, npartitions=3)

    def test_init(self):
        default_gather = Gather(
            capacity=5,
            size=5,
            npts=6,
            num_components=3,
            npartitions=3,
            member_metadata=self.md,
        )
        assert default_gather.member_data.shape == (5, 3, 6)
        compact_gather = Gather(
            capacity=5,
            size=5,
            npts=6,
            num_components=3,
            npartitions=3,
            member_metadata=self.md,
            is_compact=False,
        )
        assert compact_gather.member_data.shape == (5, 6, 3)

        es_md = Metadata(self.md_dict)
        es = TimeSeriesEnsemble(es_md, 3)
        d = TimeSeries(10)
        d = make_constant_data_ts(d)
        for i in range(3):
            es.member.append(d)

        gather_from_old = Gather(input_obj=es, npartitions=3)
        assert gather_from_old.member_data.shape == (3, 1, 5)

    def test_data(self):
        assert np.array_equal(np.array([1, 1, 1, 1, 1]), self.gather.data(0))

    def test_member(self):
        ts_member = self.gather.member(0)
        assert ts_member.live is True
        assert np.array_equal(np.array([1, 1, 1, 1, 1]), ts_member.data)

    def test_subset(self):
        ts_subset = self.gather.subset(0, 2)
        assert ts_subset.size == 2
        assert ts_subset.npartitions == 2

    def test_getitem(self):
        assert self.gather[0, 0] == 1

    def test_setitem(self):
        self.gather[0, 0] = 2
        assert self.gather[0, 0] == 2


class TestSeismogramGather:
    def setup_class(self):
        self.md_dict = {
            "delta": [5, 5],
            "starttime": [1.0, 2.0],
            "is_live": [True, False],
            "time": [2.0, 3.0],
        }
        self.md = pd.DataFrame.from_dict(self.md_dict)
        es_md = Metadata(self.md_dict)

        es = SeismogramEnsemble(es_md, 3)
        d = Seismogram(10)
        d = make_constant_data_seis(d, t0=-0.2, nsamp=6, val=2.0)
        for i in range(3):
            es.member.append(d)
        self.gather = SeismogramGather(input_obj=es, npartitions=3)

    def test_init(self):
        default_gather = SeismogramGather(
            capacity=5,
            size=5,
            npts=6,
            num_components=3,
            npartitions=3,
            member_metadata=self.md,
        )
        assert default_gather.member_data.shape == (5, 3, 6)
        compact_gather = SeismogramGather(
            capacity=5,
            size=5,
            npts=6,
            num_components=3,
            npartitions=3,
            member_metadata=self.md,
            is_compact=False,
        )
        assert compact_gather.member_data.shape == (5, 6, 3)

        es_md = Metadata(self.md_dict)

        es = SeismogramEnsemble(es_md, 3)
        d = Seismogram(10)
        d = make_constant_data_seis(d, t0=-0.2, nsamp=6, val=2.0)
        for i in range(3):
            es.member.append(d)

        gather_from_old = SeismogramGather(input_obj=es, npartitions=3)
        assert gather_from_old.member_data.shape == (3, 3, 6)

    def test_data(self):
        assert np.array_equal(
            np.array([[2, 2, 2, 2, 2, 2], [2, 2, 2, 2, 2, 2], [2, 2, 2, 2, 2, 2]]),
            self.gather.data(0),
        )

    def test_member(self):
        ts_member = self.gather.member(0)
        assert ts_member.live is True
        assert np.array_equal(
            np.array([[2, 2, 2, 2, 2, 2], [2, 2, 2, 2, 2, 2], [2, 2, 2, 2, 2, 2]]),
            ts_member.data,
        )

    def test_subset(self):
        ts_subset = self.gather.subset(0, 2)
        assert ts_subset.size == 2
        assert ts_subset.npartitions == 2

    def test_getitem(self):
        assert self.gather[0, 0, 0] == 2.0

    def test_setitem(self):
        self.gather[0, 0, 0] = 1
        assert self.gather[0, 0, 0] == 1
