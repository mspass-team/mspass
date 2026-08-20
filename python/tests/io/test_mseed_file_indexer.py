import numpy as np
from obspy import Stream, Trace, UTCDateTime, read

from mspasspy.ccore.seismic import TimeSeries
from mspasspy.ccore.utility import ErrorLogger  # noqa: F401
from mspasspy.ccore.io import _mseed_file_indexer
from mspasspy.db.database import Database


def test_gap_index_matches_merged_reader_grid(tmp_path):
    sample_rate = 20.0
    samples_per_record = 4
    missing_samples = 3
    starttime = UTCDateTime(1_610_000_000)
    header = {
        "network": "XX",
        "station": "GAP",
        "location": "00",
        "channel": "BHZ",
        "sampling_rate": sample_rate,
    }
    first = Trace(
        data=np.arange(1, samples_per_record + 1, dtype=np.int32),
        header={**header, "starttime": starttime},
    )
    second = Trace(
        data=np.arange(8, 8 + samples_per_record, dtype=np.int32),
        header={
            **header,
            "starttime": starttime
            + (samples_per_record + missing_samples) / sample_rate,
        },
    )
    mseed_file = tmp_path / "gap.mseed"
    Stream([first, second]).write(str(mseed_file), format="MSEED", reclen=256)

    index, elog = _mseed_file_indexer(str(mseed_file))
    assert len(index) == 1
    assert elog.size() == 0
    assert index[0].npts == 2 * samples_per_record + missing_samples
    assert (
        index[0].endtime == index[0].starttime + (index[0].npts - 1) / index[0].samprate
    )

    merged = read(str(mseed_file)).merge(method=0, fill_value=0)
    assert len(merged) == 1
    assert merged[0].stats.npts == index[0].npts
    assert abs(merged[0].stats.endtime.timestamp - index[0].endtime) < 1.0e-6

    segmented, _ = _mseed_file_indexer(str(mseed_file), True)
    assert [entry.npts for entry in segmented] == [samples_per_record] * 2

    datum = TimeSeries(index[0].npts)
    datum.t0 = index[0].starttime
    datum.dt = 1.0 / index[0].samprate
    Database._read_data_from_dfile(
        datum,
        str(tmp_path),
        mseed_file.name,
        index[0].foff,
        index[0].nbytes,
        format="mseed",
    )
    assert datum.live
    assert list(datum.data) == [1.0, 2.0, 3.0, 4.0, 0.0, 0.0, 0.0, 8.0, 9.0, 10.0, 11.0]
    assert datum["has_gap"]
    assert len(datum["gaps"]) == 1
    assert np.isclose(
        datum["gaps"][0]["starttime"], starttime.timestamp + 4 / sample_rate
    )
    assert np.isclose(
        datum["gaps"][0]["endtime"], starttime.timestamp + 6 / sample_rate
    )

    interpolated = TimeSeries(index[0].npts)
    interpolated.t0 = index[0].starttime
    interpolated.dt = 1.0 / index[0].samprate
    Database._read_data_from_dfile(
        interpolated,
        str(tmp_path),
        mseed_file.name,
        index[0].foff,
        index[0].nbytes,
        format="mseed",
        merge_fill_value="interpolate",
    )
    assert interpolated.live
    assert list(interpolated.data) == [
        1.0,
        2.0,
        3.0,
        4.0,
        5.0,
        6.0,
        7.0,
        8.0,
        9.0,
        10.0,
        11.0,
    ]
    assert interpolated["has_gap"]
    assert interpolated["gaps"] == datum["gaps"]
