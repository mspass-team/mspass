from pathlib import Path

import pytest

from mspasspy.ccore.io import _mseed_file_indexer
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
MSEED_FIXTURE = REPOSITORY_ROOT / "cxx" / "test" / "mseed" / "test.msd"


def test_mseed_file_indexer_binding_valid_and_empty(tmp_path):
    index, elog = _mseed_file_indexer(str(MSEED_FIXTURE), True, False)
    assert len(index) > 0
    assert elog.size() == 0
    assert index[0].sta == "E2000"
    assert index[0].chan == "VHE"
    assert index[0].foff == 0
    for segment in index:
        assert segment.nbytes > 0
        assert segment.npts > 0
        assert segment.endtime == pytest.approx(
            segment.starttime + (segment.npts - 1) / segment.samprate,
            rel=0.0,
            abs=0.0,
        )

    empty = tmp_path / "empty.mseed"
    empty.write_bytes(b"")
    empty_index, empty_elog = _mseed_file_indexer(str(empty), True, False)
    assert len(empty_index) == 0
    assert empty_elog.size() == 0


def test_mseed_file_indexer_binding_matches_native_segments_and_verbose(capfd):
    index, elog = _mseed_file_indexer(str(MSEED_FIXTURE), True, True)
    captured = capfd.readouterr()

    assert elog.size() == 0
    assert [
        (
            segment.net,
            segment.sta,
            segment.loc,
            segment.chan,
            segment.foff,
            segment.nbytes,
            segment.npts,
            segment.samprate,
        )
        for segment in index
    ] == [
        ("X6", "E2000", "", "VHE", 0, 4096, 70, 0.1),
        ("X6", "E2000", "", "VHE", 4096, 1916928, 2434403, 0.1),
        ("X6", "A2000", "", "UHE", 1921024, 4096, 15, 0.01),
        ("X6", "A2000", "", "UHE", 1925120, 57344, 47205, 0.01),
        ("X6", "A2000", "", "UHE", 1982464, 229376, 200030, 0.01),
    ]
    diagnostic_lines = captured.err.splitlines()
    assert captured.out == ""
    assert len(diagnostic_lines) == 3
    assert all("time tear at packet" in line for line in diagnostic_lines)
    assert all("previous expected time" in line for line in diagnostic_lines)
    assert all("actual start" in line for line in diagnostic_lines)

    quiet_index, quiet_elog = _mseed_file_indexer(str(MSEED_FIXTURE), True, False)
    quiet_output = capfd.readouterr()
    assert len(quiet_index) == len(index)
    assert quiet_elog.size() == 0
    assert quiet_output.out == ""
    assert quiet_output.err == ""


@pytest.mark.parametrize("damage_position", ["before", "after"])
def test_mseed_file_indexer_binding_rejects_corruption_without_partial_result(
    tmp_path, damage_position
):
    complete_record = MSEED_FIXTURE.read_bytes()[:4096]
    corruption = b"\x7f" * 64
    if damage_position == "before":
        payload = corruption + complete_record
    else:
        payload = complete_record + corruption
    damaged = tmp_path / f"damaged_{damage_position}.mseed"
    damaged.write_bytes(payload)

    with pytest.raises(MsPASSError) as excinfo:
        _mseed_file_indexer(str(damaged), True, False)
    assert excinfo.value.severity == ErrorSeverity.Invalid
    assert "ms3_readmsr_r" in str(excinfo.value)
