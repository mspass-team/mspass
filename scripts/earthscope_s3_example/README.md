# Bounded EarthScope S3 day-window example

This directory is a maintained example for one specific workflow: index
EarthScope station-day miniSEED objects, extract event windows, and write
bounded station batches.  It is not a universal EarthScope data API.

The example contains no credentials or notebook output.  Do not print, save,
commit, or place short-lived credentials in Dask task arguments.  Register one
client per worker instead.  The plugin uses boto3's standard credential chain,
which lets the same GeoLab worker identity read the public input bucket and
write its authorized scratch bucket:

```python
from scripts.earthscope_s3_example.worker import EarthScopeS3Worker

dask_client.register_plugin(EarthScopeS3Worker())
```

The input is intentionally limited to the AWS Open Data Program bucket,
`earthscope-geophysical-data`, for networks AK, II, IU, N4, PB, TA, UU, and
UW.  In particular, this is the correct current bucket for the 2014 TA data
that motivated the example, and it does not require EarthScope temporary
credentials.  Other networks use a separately authorized EarthScope
Repository access point and network-scoped credentials; adapt the example only
after following the
[EarthScope direct-S3 guide](https://docs.earthscope.org/sdk/s3-direct-access-tutorial)
for its current bucket, region, and credential rules.

For serial indexing, create a standard `boto3.Session` at runtime and pass it
to `fetch_s3_client(session=...)`.  Use `index_days_for_year(year)` when
building the station-day index; it includes the adjacent December 31 and
January 1 needed by padded windows.  `year_query(year)` is half-open, so an
arrival at exactly January 1 belongs to only the new year.  Filter network
values with `normalized_networks` instead of deleting an arbitrary element
returned by MongoDB `distinct`.  Normalize every arrival with
`normalize_station`; a station absent from the cross-reference keeps its
original station code and receives the configured default network instead of
inheriting stale loop state:

```python
from scripts.earthscope_s3_example.workflow import normalize_station

arrival_documents = [
    normalize_station(document, station_cross_reference, default_network="TA")
    for document in arrival_documents
]
```

Build station batches with a finite `max_arrivals_per_batch`.  Each arrival
identity appears once, while its batch unions every station-day object needed
by its window:

```python
from scripts.earthscope_s3_example.workflow import build_station_batches

batches = build_station_batches(
    arrival_documents,
    db.wf_s3,
    -240,
    300,
    pad=100,
    max_arrivals_per_batch=32,
    auxiliary_keys=("evid", "orid", "iphase", "delta", "pick_channel"),
)
```

The worker reader returns a local generator.  The completion consumes that
generator in the same worker, writes independent pickle records to a bounded
temporary file, and uploads with one multipart buffer.  It never constructs a
second whole-day ensemble.  Output keys derive from the station, day, batch
number, and window identities, so retrying the same batch overwrites the same
object rather than duplicating it.

```python
import os

from scripts.earthscope_s3_example.workflow import run_station_batches

statuses = run_station_batches(
    batches,
    dask_client,
    output_bucket=os.environ["SCRATCH_BUCKET"],
    sliding_window_size=1,
)
```

`output_bucket` is the plain S3 bucket name.  Put any directory-like portion
in `output_prefix`; do not pass an `s3://` URI as the bucket argument.

Keep `sliding_window_size=1` until the largest station batch has been measured
on the target GeoLab deployment.  Increase it only when the measured worker
and database headroom permits.  A station-day miniSEED image, one decoded
station stream, and one event window can coexist in a worker; the batch limit
must make that peak fit.

The object format starts with a small header followed by independently pickled
waveform records.  Read it incrementally with `iter_record_stream`; never load
untrusted pickle data.  The first yielded object is the header and the
remaining objects are waveform records.

The automated tests use fake MongoDB and S3 implementations.  They do not
validate EarthScope authorization, the live repository endpoint, real CSS
tables, GeoLab filesystem limits, or production multipart throughput.  Run a
small measured day before a yearly import.
