import numpy as np
from obspy import Stream, Trace, UTCDateTime, read

from mspasspy.ccore.utility import ErrorLogger  # noqa: F401
from mspasspy.ccore.io import _mseed_file_indexer


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
        data=np.arange(samples_per_record, dtype=np.int32),
        header={**header, "starttime": starttime},
    )
    second = Trace(
        data=np.arange(samples_per_record, dtype=np.int32),
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
