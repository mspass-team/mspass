"""Bounded station-batch workflow for EarthScope station-day miniSEED."""

import calendar
from dataclasses import dataclass
import hashlib
import io
import json
from pathlib import Path
import pickle
import tempfile
from urllib.parse import quote

import obspy
from obspy import UTCDateTime
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from mspasspy.algorithms.signals import detrend
from mspasspy.algorithms.window import WindowData
from mspasspy.util.converter import Stream2TimeSeriesEnsemble
from mspasspy.workflow import sliding_window_pipeline

from .worker import fetch_s3_client

EARTHSCOPE_BUCKET = "earthscope-mseed-res-na3mtd4fq5kz7pntcyr1uh46use2a--ol-s3"
RECORD_STREAM_MAGIC = b"MSPASS_EARTHSCOPE_STATION_BATCH_V1\n"
IMMUTABLE_METADATA = {
    "delta",
    "endtime",
    "npts",
    "starttime",
    "time_standard",
    "utc_convertible",
}


@dataclass(frozen=True)
class WindowRequest:
    """One arrival window retained exactly once in a station batch."""

    arrival_id: str
    arrival_time: float
    starttime: float
    endtime: float
    metadata: dict


@dataclass(frozen=True)
class StationBatch:
    """A bounded set of windows for one station and arrival day."""

    year: int
    jday: int
    net: str
    sta: str
    batch_index: int
    arrivals: tuple
    object_keys: tuple
    missing_days: tuple
    channel_select: str = "B*"


@dataclass
class StationRecordStream:
    """Worker-local record iterator paired with its small batch description."""

    batch: StationBatch
    records: object


def year_query(year, time_key="Ptime"):
    """Return a half-open MongoDB time query for one calendar year."""
    start = float(UTCDateTime(year=year, month=1, day=1))
    end = float(UTCDateTime(year=year + 1, month=1, day=1))
    return {time_key: {"$gte": start, "$lt": end}}


def normalized_networks(values):
    """Remove explicitly empty network values without relying on ordering."""
    return sorted(
        {
            str(value).strip()
            for value in values
            if value is not None and str(value).strip()
        }
    )


def normalize_station(document, station_cross_reference, default_network="TA"):
    """Normalize network/station codes while preserving an unmatched station."""
    result = dict(document)
    station = result.get("sta")
    if not station:
        raise ValueError("arrival document is missing sta")
    if station in station_cross_reference:
        network, final_station = station_cross_reference[station]
        result["net"] = network
        result["sta"] = final_station
    else:
        result["net"] = result.get("net") or default_network
        result["sta"] = station
    return result


def days_for_interval(starttime, endtime):
    """Return every UTC year/julian-day pair touched by a closed interval."""
    if endtime < starttime:
        raise ValueError("endtime must not precede starttime")
    first = UTCDateTime(starttime)
    current = UTCDateTime(year=first.year, julday=first.julday)
    stop = UTCDateTime(endtime)
    days = []
    while current <= stop:
        days.append((current.year, current.julday))
        current += 86400
    return tuple(days)


def index_days_for_year(year):
    """Yield the target year plus the adjacent day needed by padded windows."""
    previous_year_days = 366 if calendar.isleap(year - 1) else 365
    yield year - 1, previous_year_days
    days = 366 if calendar.isleap(year) else 365
    for jday in range(1, days + 1):
        yield year, jday
    yield year + 1, 1


def list_station_day_keys(
    s3_client,
    net,
    year,
    jday,
    bucket=EARTHSCOPE_BUCKET,
):
    """List all station-day base keys using the S3 paginator."""
    prefix = f"miniseed/{net}/{year}/{jday:03d}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for item in page.get("Contents", ()):
            keys.add(item["Key"].split("#", 1)[0])
    return sorted(keys)


def station_day_document(key):
    """Convert a canonical EarthScope station-day key to an index document."""
    parts = key.split("/")
    if len(parts) < 5:
        raise ValueError(f"invalid EarthScope station-day key: {key!r}")
    filename = parts[-1].split(".")
    if len(filename) < 4:
        raise ValueError(f"invalid EarthScope station-day filename: {key!r}")
    year = int(parts[-3])
    jday = int(parts[-2])
    starttime = float(UTCDateTime(year=year, julday=jday))
    return {
        "s3key": key,
        "net": parts[-4],
        "sta": filename[0],
        "year": year,
        "jday": jday,
        "starttime": starttime,
        "endtime": starttime + 86400.0,
    }


def index_station_days(
    s3_client,
    collection,
    networks,
    days,
    bucket=EARTHSCOPE_BUCKET,
):
    """Upsert a deterministic station-day index into a Mongo-like collection."""
    indexed = 0
    indexed_days = tuple(days)
    for net in normalized_networks(networks):
        for year, jday in indexed_days:
            keys = list_station_day_keys(s3_client, net, year, jday, bucket)
            for key in keys:
                document = station_day_document(key)
                collection.replace_one({"s3key": key}, document, upsert=True)
                indexed += 1
    return {"indexed": indexed}


def _anchor_day(timestamp):
    time = UTCDateTime(timestamp)
    return time.year, time.julday


def _holding_keys(collection, net, sta, days):
    keys = set()
    missing = []
    for year, jday in sorted(days):
        query = {"net": net, "sta": sta, "year": year, "jday": jday}
        day_keys = {document["s3key"] for document in collection.find(query)}
        if day_keys:
            keys.update(day_keys)
        else:
            missing.append((year, jday))
    return tuple(sorted(keys)), tuple(missing)


def build_station_batches(
    arrivals,
    holdings_collection,
    start,
    end,
    *,
    pad=100.0,
    max_arrivals_per_batch=32,
    arrival_id_key="arid",
    time_key="time",
    auxiliary_keys=(),
    channel_select="B*",
):
    """Build bounded station batches without arrival/holding cross products."""
    if max_arrivals_per_batch <= 0:
        raise ValueError("max_arrivals_per_batch must be positive")
    if end < start:
        raise ValueError("end must not precede start")

    groups = {}
    identities = {}
    for arrival in arrivals:
        arrival_id = str(arrival[arrival_id_key])
        net_value = arrival.get("net")
        sta_value = arrival.get("sta")
        if not net_value or not sta_value:
            raise ValueError("arrival documents require nonempty net and sta")
        net = str(net_value)
        sta = str(sta_value)
        arrival_time = float(arrival[time_key])
        year, jday = _anchor_day(arrival_time)
        group_key = (year, jday, net, sta)
        identity_key = (net, sta, arrival_id)
        request = WindowRequest(
            arrival_id=arrival_id,
            arrival_time=arrival_time,
            starttime=arrival_time + start - pad,
            endtime=arrival_time + end + pad,
            metadata={key: arrival[key] for key in auxiliary_keys if key in arrival},
        )
        previous = identities.get(identity_key)
        if previous is not None:
            if previous != request:
                raise ValueError(
                    f"conflicting rows for arrival identity {arrival_id!r}"
                )
            continue
        identities[identity_key] = request
        groups.setdefault(group_key, []).append(request)

    batches = []
    for (year, jday, net, sta), requests in sorted(groups.items()):
        requests.sort(key=lambda item: (item.arrival_time, item.arrival_id))
        for batch_index, offset in enumerate(
            range(0, len(requests), max_arrivals_per_batch)
        ):
            batch_requests = tuple(requests[offset : offset + max_arrivals_per_batch])
            needed_days = {
                day
                for request in batch_requests
                for day in days_for_interval(request.starttime, request.endtime)
            }
            object_keys, missing_days = _holding_keys(
                holdings_collection, net, sta, needed_days
            )
            batches.append(
                StationBatch(
                    year=year,
                    jday=jday,
                    net=net,
                    sta=sta,
                    batch_index=batch_index,
                    arrivals=batch_requests,
                    object_keys=object_keys,
                    missing_days=missing_days,
                    channel_select=channel_select,
                )
            )
    return batches


def _not_found(error):
    code = str(error.response.get("Error", {}).get("Code", ""))
    return code in {"404", "NoSuchKey", "NotFound"}


def read_versioned_miniseed(
    s3_client,
    base_key,
    *,
    bucket=EARTHSCOPE_BUCKET,
    max_version=10,
    stream_reader=None,
):
    """Read one versioned object, close its body, and preserve real failures."""
    if (
        isinstance(max_version, bool)
        or not isinstance(max_version, int)
        or max_version < 0
    ):
        raise ValueError("max_version must be a nonnegative integer")
    selected_key = None
    candidates = [base_key]
    candidates.extend(f"{base_key}#{version}" for version in range(max_version, 0, -1))
    for candidate in candidates:
        try:
            s3_client.head_object(Bucket=bucket, Key=candidate)
        except ClientError as error:
            if _not_found(error):
                continue
            raise
        selected_key = candidate
        break
    if selected_key is None:
        raise FileNotFoundError(f"no readable version found for S3 key {base_key!r}")

    response = s3_client.get_object(Bucket=bucket, Key=selected_key)
    body = response["Body"]
    try:
        data = body.read()
    except BaseException:
        try:
            body.close()
        except Exception:
            pass
        raise
    else:
        body.close()

    reader = stream_reader or obspy.read
    return reader(io.BytesIO(data), format="mseed")


def prepare_stream(
    stream,
    *,
    channel_select="B*",
    detrend_type=None,
    converter=None,
    detrend_function=None,
):
    """Merge each sample rate, convert once, and detrend the decoded data."""
    selected = stream.select(channel=channel_select) if channel_select else stream
    merged = obspy.Stream()
    rates = sorted({trace.stats.sampling_rate for trace in selected})
    for rate in rates:
        same_rate = selected.select(sampling_rate=rate).copy()
        same_rate.merge(method=1)
        merged += same_rate
    convert = converter or Stream2TimeSeriesEnsemble
    decoded = convert(merged)
    if detrend_type is not None:
        apply_detrend = detrend_function or detrend
        decoded = apply_detrend(decoded, type=detrend_type)
    return decoded


def live_nonempty(record):
    """Return True only for a live waveform with at least one sample."""
    if hasattr(record, "dead") and record.dead():
        return False
    if hasattr(record, "live") and not record.live:
        return False
    npts = getattr(record, "npts", None)
    if npts is not None:
        return npts > 0
    data = getattr(record, "data", None)
    return data is None or len(data) > 0


def iter_station_records(
    batch,
    *,
    input_bucket=EARTHSCOPE_BUCKET,
    detrend_type="simple",
    short_segment_handling="kill",
    worker_data_key="earthscope_s3_client",
    max_version=10,
):
    """Yield live records for one bounded batch without making a day ensemble."""
    s3_client = fetch_s3_client(worker_data_key=worker_data_key)
    stream = obspy.Stream()
    for key in batch.object_keys:
        stream += read_versioned_miniseed(
            s3_client,
            key,
            bucket=input_bucket,
            max_version=max_version,
        )
    if not batch.object_keys:
        return
    decoded = prepare_stream(
        stream,
        channel_select=batch.channel_select,
        detrend_type=detrend_type,
    )
    for request in batch.arrivals:
        ensemble = WindowData(
            decoded,
            request.starttime,
            request.endtime,
            short_segment_handling=short_segment_handling,
        )
        for record in ensemble.member:
            if not live_nonempty(record):
                continue
            record["arrival_id"] = request.arrival_id
            record["arrival_time"] = request.arrival_time
            for key, value in request.metadata.items():
                if key not in IMMUTABLE_METADATA:
                    record[key] = value
            yield record
        # Drop references into the previous window before constructing the
        # next one.  pybind member wrappers can otherwise keep the ensemble's
        # native vector alive across the next WindowData call.
        record = None
        del ensemble


def read_station_batch(batch, **kwargs):
    """Create an intentionally worker-local iterator for a station batch."""
    return StationRecordStream(
        batch=batch,
        records=iter_station_records(batch, **kwargs),
    )


def station_batch_object_key(batch, prefix="earthscope-windowed"):
    """Return the deterministic key used by every retry of the same batch."""
    identity = {
        "year": batch.year,
        "jday": batch.jday,
        "net": batch.net,
        "sta": batch.sta,
        "batch_index": batch.batch_index,
        "channel_select": batch.channel_select,
        "windows": [
            [request.arrival_id, request.starttime, request.endtime]
            for request in batch.arrivals
        ],
    }
    digest = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:16]
    path = "/".join(
        (
            prefix.strip("/"),
            str(batch.year),
            f"{batch.jday:03d}",
            quote(batch.net, safe=""),
            quote(batch.sta, safe=""),
            f"batch-{batch.batch_index:05d}-{digest}.pklstream",
        )
    )
    return path.lstrip("/")


def _record_header(batch):
    return {
        "format": "mspass-earthscope-station-batch-v1",
        "year": batch.year,
        "jday": batch.jday,
        "net": batch.net,
        "sta": batch.sta,
        "batch_index": batch.batch_index,
        "arrival_ids": [request.arrival_id for request in batch.arrivals],
        "missing_days": list(batch.missing_days),
    }


def write_station_batch_records(
    station_stream,
    *,
    output_bucket,
    output_prefix="earthscope-windowed",
    worker_data_key="earthscope_s3_client",
    multipart_chunk_bytes=16 * 1024 * 1024,
):
    """Write independent records to disk, upload them, and return small status."""
    batch = station_stream.batch
    object_key = station_batch_object_key(batch, output_prefix)
    record_count = 0
    with tempfile.TemporaryDirectory(prefix="mspass-earthscope-") as directory:
        output_path = Path(directory) / "station-batch.pklstream"
        with output_path.open("wb") as output:
            output.write(RECORD_STREAM_MAGIC)
            pickle.dump(_record_header(batch), output, protocol=pickle.HIGHEST_PROTOCOL)
            for record in station_stream.records:
                if not live_nonempty(record):
                    continue
                pickle.dump(record, output, protocol=pickle.HIGHEST_PROTOCOL)
                record_count += 1
            # Do not retain the last member wrapper during multipart upload.
            record = None
        byte_count = output_path.stat().st_size
        config = TransferConfig(
            multipart_threshold=multipart_chunk_bytes,
            multipart_chunksize=multipart_chunk_bytes,
            max_concurrency=1,
            use_threads=False,
        )
        s3_client = fetch_s3_client(worker_data_key=worker_data_key)
        s3_client.upload_file(
            str(output_path), output_bucket, object_key, Config=config
        )

    missing_count = len(batch.missing_days)
    return {
        "ok": missing_count == 0,
        "bucket": output_bucket,
        "key": object_key,
        "records": record_count,
        "bytes": byte_count,
        "missing_days": missing_count,
    }


def iter_record_stream(stream):
    """Yield a record-stream header and records without materializing the file."""
    if stream.read(len(RECORD_STREAM_MAGIC)) != RECORD_STREAM_MAGIC:
        raise ValueError("not an EarthScope station-batch record stream")
    while True:
        try:
            yield pickle.load(stream)
        except EOFError:
            return


def run_station_batches(
    batches,
    dask_client,
    *,
    output_bucket,
    input_bucket=EARTHSCOPE_BUCKET,
    output_prefix="earthscope-windowed",
    sliding_window_size=1,
    max_version=10,
):
    """Run the bounded worker-side reader/writer and return small statuses."""
    return sliding_window_pipeline(
        batches,
        read_station_batch,
        dask_client,
        sliding_window_size=sliding_window_size,
        pfunc_kwargs={"input_bucket": input_bucket, "max_version": max_version},
        completion_function=write_station_batch_records,
        cfunc_kwargs={
            "output_bucket": output_bucket,
            "output_prefix": output_prefix,
        },
        completion_on_worker=True,
        retain_results=True,
    )
