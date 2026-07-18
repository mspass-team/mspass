.. _continuous_data:

Continuous Data Handling with MsPASS
==========================================
Concepts
~~~~~~~~~~~
Seismologists universally use the term "continuous data" to mean
long time series records with no significant gaps.   The term is
actually potentially confusing because the data are not really continuous
at all but sampled at a fixed sample interval, nor are they of infinite duration
which might be implied if you were discussing Fourier transforms on functions.
In practice, "continuous data" in seismology means a series of
windowed segments that can be merged to form a longer window of data.
The maximum length possible is the duration of data recording.
The process of gluing (merging) multiple segments is, more or less,
the inverse of cutting a shorter window out of a longer window of data.
Gluing/merging data algorithms have to deal with some different issues
than windowing.

Some important issues about common practice and the reality of real
data are:

#.  There are two common choices for how the data are blocked:
    (1) day volumes, and (2)  raw digitizer files of irregular length
    created when the digitizer does a memory dump (All current
    generation digitizers write to an internal memory buffer that is
    dumped when the memory use exceeds a high water mark.)  In either case
    there is some explicit or implicit (e.g. file naming convention)
    that provides a hint of the order of the segments.

#.  A large fraction of data contain various types of "data gaps".
    Gaps occur for a long list of reasons that are mostly unimportant
    when analyzing such data.  What is important is that data gaps
    span a range of time scales from a single sample to years.

#.  A less-common problem is a data overlap.  An overlap occurs when
    two segments you need to merge have conflicting time stamps.
    To make this clear it is helpful to review two MsPASS concepts in
    the TimeSeries and Seismogram data objects.  Let *d1* and *d2* be
    two :py:class:`TimeSeries<mspasspy.ccore.seismic.TimeSeries>` objects that are successive segments we
    expect to merge with *d2* being the segment following *d1* in time.
    In MsPASS we use the attribute *t0* (alternatively the method
    *starttime*) for the time of sample 0.  We also use the method
    *endtime* to get the computed time of the last data sample.  An
    overlap is present between these two segments when
    *d2.t0 < d1.endtime() + 0.5*d1.dt*.  The half-sample tolerance is the
    one used by the native splicing implementation.
    We know of three ways overlaps can
    occur:  (1) timing problems with the instrument that recorded the
    data, (2) hardware or software issues in recording instrument that
    cause packets to be duplicated in memory before they are dumped, and
    (3) blunders in data management where duplicate files are indexed
    and defined in a database wf collection (wfdisc table in Antelope).

MsPASS has some capabilities for merging data within the realities of
real data noted above.   These are discussed in the section below.
MsPASS does not, however, substitute for nitty-gritty details network
and experiment operators have to face in cleaning field data for archive.
We consider that as a problem already solved by EarthScope,
the USGS, and global network operators in systems they
use for creating the modern data archive of the FDSN.  Custom
experimental data may need to utilize EarthScope tools to fix problems not
covered by MsPASS.

Gap Processing
~~~~~~~~~~~~~~~~~~
Internally MsPASS handles data gaps with a subclass of the
:py:class:`TimeSeries<mspasspy.ccore.seismic.TimeSeries>` called
:py:class:`TimeSeriesWGaps<mspasspy.ccore.seismic.TimeSeriesWGaps>`.  That extension of
:py:class:`TimeSeries<mspasspy.ccore.seismic.TimeSeries>` is written in C++ and is documented
`here <https://www.mspass.org/cxx_api/mspass.html#mspass-namespace>`__.
Like :py:class:`TimeSeries<mspasspy.ccore.seismic.TimeSeries>` this class has Python bindings created
with pybind11.  The gap-query and editing methods described in the C++ documentation
page have python bindings.  There are methods for defining gaps,
zeroing data in defined gaps, and deleting gaps.
See the doxygen pages linked above for details.
The Python functions that currently deal with data gaps have a second
strategy for handling this problem, best described in the context
of those functions.


Merging Data Segments
~~~~~~~~~~~~~~~~~~~~~~~~
There are currently two different methods in MsPASS to handle merging
continuous data segments:  (1) a special, implicit option of the
:py:meth:`read_data<mspasspy.db.database.Database.read_data>` method of the
:py:class:`Database<mspasspy.db.database.Database>` class, and (2) the
processing function :py:func:`merge<mspasspy.algorithms.window.merge>`.
In addition, there is a special reader function called
:py:func:`TimeIntervalReader<mspasspy.db.ensembles.TimeIntervalReader>` that can be used
to read fixed time windows of data.  That function uses
:py:func:`merge<mspasspy.algorithms.window.merge>`
to do gap and overlap repair.

read_data merge algorithm
---------------------------
This approach is only relevant if you have raw miniseed files you
plan to read to initiate your processing sequence.  The miniseed format
uses a packet structure with each packet normally defining a single
channel (Note the standard allows multiplexed data but none of us have
ever encountered such data.).   The order of the packets is used by
all readers we know of to determine if a sequence of packets are a single
waveform.   If the station codes ("net", "sta", "chan", and "loc" attributes
in all MsPASS schemas) change in a sequence of packets readers
universally assume that is the end of a given segment.  How readers handle
a second issue is, however, variable.  Each miniSEED packet has a time
tag comparable to the ``t0`` attribute of a
:py:class:`TimeSeries<mspasspy.ccore.seismic.TimeSeries>` object and a
sample count and rate from which an end time can be computed.  If the
``t0`` value of a packet is greater than some
fractional tolerance of 1 sample more than the endtime of the previous
packet, a reader will invoke a gap handler.  A reader's gap handler
commonly has options for what to do with different kinds of "gaps", but
for this section our definition is defined by the way ObsPy
handles this problem with its
:py:meth:`Stream.merge<obspy.core.stream.Stream.merge>` method described
`here <https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream.merge.html>`__.
That algorithm is relevant when a miniSEED index document spans time tears,
which can happen when
:py:meth:`index_mseed_file<mspasspy.db.database.Database.index_mseed_file>` is
run with ``segment_time_tears=False``.  The current default is ``True``.
If you use this approach, you will
need to also take care in defining the value of the following arguments
that are passed to ObsPy's merge function for gap handling:
``merge_method``, ``merge_fill_value``, and
``merge_interpolation_samples``.  Those three arguments are passed directly
to the ObsPy merge arguments ``method``, ``fill_value``, and
``interpolation_samples``.

Users who already rely on ObsPy for this functionality may instead write a
custom function that calls ``Stream.merge`` directly rather than using the
merge implied by ``read_data``.

MsPASS merge function
-----------------------
MsPASS has a native version of a function with capabilities similar to the
ObsPy merge function noted above.  The MsPASS function adds features and
delegates its core splicing and overlap-repair work to C++ implementations.
The docstring for :py:func:`merge<mspasspy.algorithms.window.merge>` describes more
details but some key features of this function are:

- Like ObsPy's function of the same name its purpose is to glue/merge
  a set of waveform components into a single, continuous time series.
  A key difference is that the ObsPy function requires a ``Stream`` object as
  input while the MsPASS function accepts any iterable of MsPASS
  ``TimeSeries`` objects.  The ``member`` container of a
  :py:class:`TimeSeriesEnsemble<mspasspy.ccore.seismic.TimeSeriesEnsemble>`
  is a convenient input.
- It provides for an optional windowing of the merged result.  That approach
  is useful, for example, for carving events out from a local archive of
  continuous waveform data in a single step. This feature is useful for
  reducing the memory footprint of a parallel job.
- Gap handling is controlled by ``zero_gaps``.  With its default value of
  ``False``, a gap in the requested output interval causes the result to be
  marked dead.  With ``zero_gaps=True``, gap samples are set to zero and the
  live result has ``has_gap=True`` plus a ``gaps`` Metadata value.  ``gaps``
  is a list of dictionaries whose ``starttime`` and ``endtime`` values define
  each gap in epoch time.
- Overlap handling is controlled by another boolean parameter
  with the name "fix_overlaps".   When set True the function will
  check for overlapping data and attempt to repair overlaps only if
  the overlapping samples match within the implementation tolerance.
  With the default ``False`` setting, overlaps are invalid input and can
  cause the merge to fail.  ObsPy uses a less dogmatic algorithm driven by an optional
  function argument called "interpolation_samples".  As noted above it has
  been our experience that, in general, overlapping data always indicate
  a data quality problem that invalidates the data when the samples
  do not match.  If you need
  the ObsPy functionality use the
  :py:func:`TimeSeriesEnsemble2Stream<mspasspy.util.converter.TimeSeriesEnsemble2Stream>` and the
  inverse :py:func:`Stream2TimeSeriesEnsemble<mspasspy.util.converter.Stream2TimeSeriesEnsemble>`
  to create the ObsPy input and then restore the returned data to
  the MsPASS internal data structures

TimeIntervalReader
-----------------------
A second MsPASS tool for working with continuous data is a function
with the descriptive name
:py:func:`TimeIntervalReader<mspasspy.db.ensembles.TimeIntervalReader>`.
It is designed to do the high-level task of cutting a fixed time
interval of data from one or more channels of a continuous data archive.
This function is built on top of the lower-level
:py:func:`merge<mspasspy.algorithms.window.merge>` but is best thought of as
an alternative reader to create ensembles cut from a continuous data archive.
For that reason the required arguments are a database handle and the
time interval of data to be extracted from the archive.  Gap and overlap
handling is delegated to :py:func:`merge<mspasspy.algorithms.window.merge>`.
The return is a list of ``TimeSeriesEnsemble`` objects grouped by channel and
location code.  By default, failed merged waveforms are discarded; set
``save_zombies=True`` to retain dead members for diagnosis.

Examples
------------
*Example 1: Create a single waveform in a defined time window from a
continuous data archive.*

This example creates a longer
:py:class:`TimeSeries<mspasspy.ccore.seismic.TimeSeries>` from indexed day
files for the BHZ channel of GSN station AAK.  Replace the example time range
with one covered by your archive.  The symbol ``db`` is assumed to be an
existing MsPASS database handle.

.. code-block:: python

    from bson import json_util
    from obspy import UTCDateTime
    from pymongo import ASCENDING

    from mspasspy.algorithms.window import merge
    from mspasspy.ccore.utility import MsPASSError

    net = "II"
    sta = "AAK"
    chan = "BHZ"
    loc = "00"  # STS-1 sensor at AAK
    output_starttime = UTCDateTime("2010-01-01T00:00:00").timestamp
    output_endtime = output_starttime + 3600.0

    # Retrieve every segment that intersects the requested interval.
    query = {
        "sta": sta,
        "net": net,
        "chan": chan,
        "loc": loc,
        "starttime": {"$lte": output_endtime},
        "endtime": {"$gte": output_starttime},
    }
    cursor = db.wf_miniseed.find(query).sort([("starttime", ASCENDING)])
    segments = db.read_data(cursor, collection="wf_miniseed")

    if segments.dead():
        print("The following query yielded no usable data:")
        print(json_util.dumps(query, indent=2))
    else:
        try:
            merged_data = merge(
                segments.member,
                starttime=output_starttime,
                endtime=output_endtime,
            )
        except MsPASSError as err:
            print("Merge rejected the input segments:", err)
        else:
            if merged_data.live:
                print("Output is live and has", merged_data.npts, "samples")
            else:
                print("A gap or overlap caused the merged datum to be killed")

*Example 2: Parallel read from a continuous archive.*

This workflow prototype builds waveform windows around previously measured P
arrivals.  It intentionally leaves ``arrival2list`` application-specific;
that function must return a list of dictionaries containing ``net``, ``sta``,
and an epoch-time ``arrival_time``.  Each dictionary drives one MongoDB query
and is also copied into the Metadata of the corresponding ensemble through
``container_to_merge``.

.. code-block:: python

    import dask.bag as dbag

    from mspasspy.algorithms.window import WindowData, merge
    from mspasspy.ccore.seismic import TimeSeriesEnsemble
    from mspasspy.ccore.utility import Metadata
    from mspasspy.client import Client
    from mspasspy.io.distributed import (
        read_distributed_data,
        write_distributed_data,
    )

    mspass_client = Client()
    dask_client = mspass_client.get_scheduler()
    continuous_db = mspass_client.get_database("TA2010")
    output_db = mspass_client.get_database("Pdata2010")

    def query_generator(doc, window_start, window_end):
        """Build a query for segments intersecting one arrival window."""
        arrival_time = float(doc["arrival_time"])
        return {
            "net": doc["net"],
            "sta": doc["sta"],
            "starttime": {"$lte": arrival_time + window_end},
            "endtime": {"$gte": arrival_time + window_start},
        }

    def make_segments(ensemble, window_start, window_end):
        """Merge each channel/location group around one arrival."""
        output = TimeSeriesEnsemble(Metadata(ensemble), len(ensemble.member))
        if ensemble.dead():
            return output

        arrival_time = float(ensemble["arrival_time"])
        starttime = arrival_time + window_start
        endtime = arrival_time + window_end

        groups = {}
        for datum in ensemble.member:
            loc = datum["loc"] if datum.is_defined("loc") else None
            groups.setdefault((datum["chan"], loc), []).append(datum)

        for segments in groups.values():
            segments.sort(key=lambda datum: datum.t0)
            if len(segments) == 1:
                datum = WindowData(segments[0], starttime, endtime)
            else:
                datum = merge(
                    segments,
                    starttime=starttime,
                    endtime=endtime,
                    fix_overlaps=True,
                )
            output.member.append(datum)

        if any(datum.live for datum in output.member):
            output.set_live()
        return output

    # Application-specific: return dictionaries with net, sta, arrival_time.
    arrival_list = arrival2list(args)
    if not arrival_list:
        raise ValueError("arrival2list returned no arrivals")

    window_start = -100.0
    window_end = 300.0
    query_list = [
        query_generator(doc, window_start, window_end) for doc in arrival_list
    ]

    # Matching partition counts preserve one-to-one correspondence between
    # query results and the arrival Metadata copied with container_to_merge.
    npartitions = min(20, len(arrival_list))
    arrival_bag = dbag.from_sequence(arrival_list, npartitions=npartitions)
    waveform_bag = read_distributed_data(
        query_list,
        db=continuous_db,
        collection="wf_miniseed",
        scheduler="dask",
        npartitions=npartitions,
        container_to_merge=arrival_bag,
    )
    segment_bag = waveform_bag.map(make_segments, window_start, window_end)

    # The bag contains ensembles, so data_are_atomic must be False.  Supplying
    # dask_client also ensures execution uses the configured distributed client.
    output_ids = write_distributed_data(
        segment_bag,
        output_db,
        data_are_atomic=False,
        collection="wf_TimeSeries",
        scheduler="dask",
        data_tag="TA2010_P_windows",
        dask_client=dask_client,
    )

Parallel I/O matters here because a real archive may be driven by millions of
picks.  The main steps are:

1.  ``arrival2list`` loads or constructs the arrival Metadata.  One possible
    source is the CSS3.0 tables in the
    `ANF CSS Event Database archive <https://doi.org/10.17611/DP/EB.1>`__.  A pandas
    table or a MongoDB ``arrival`` collection could provide the same three
    required values.
2.  ``query_generator`` constructs one waveform-overlap query per arrival.
    ``arrival_bag`` retains the same ordered Metadata in the parallel
    container expected by ``container_to_merge``.
3.  A list of query dictionaries puts ``read_distributed_data`` in ensemble
    mode.  ``container_to_merge`` performs a one-to-one Metadata merge, so
    each returned ensemble receives the arrival time that drove its query.
4.  ``make_segments`` groups each ensemble by channel and location, sorts the
    group by ``t0``, and either windows its single member or merges multiple
    members.
5.  ``write_distributed_data`` is told explicitly that the bag contains
    ensembles and is given the configured Dask client for execution.

*Example 3: Application of TimeIntervalReader.*

This example assumes ``shottimes.txt`` contains one epoch time per line from
an onshore-offshore airgun experiment or a set of land shots.  The script is
serial, but the same operation can be mapped over shot times in a parallel
workflow.

.. code-block:: python

    from mspasspy.db.client import DBClient
    from mspasspy.db.ensembles import TimeIntervalReader

    dbclient = DBClient()
    db = dbclient.get_database("my_continuous_dataset")
    window_start = 0.0
    window_end = 50.0  # cut 50 s listen windows

    with open("shottimes.txt", encoding="utf-8") as stream:
        for line in stream:
            line = line.strip()
            if not line:
                continue
            shot_time = float(line)
            ensemble_list = TimeIntervalReader(
                db,
                shot_time + window_start,
                shot_time + window_end,
                fix_overlaps=True,
            )
            for ensemble in ensemble_list:
                for datum in ensemble.member:
                    db.save_data(datum, collection="wf_TimeSeries")
