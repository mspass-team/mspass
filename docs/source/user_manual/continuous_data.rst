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
    two :code:`TimeSeries` objects that are successive segments we
    expect to merge with *d2* being the segment following *d1* in time.
    In MsPASS we use the attribute *t0* (alternatively the method
    *starttime*) for the time of sample 0.  We also use the method
    *endtime* to get the computed time of the last data sample.  An
    overlap is present between these two segments when
    *d2.t0 < d1.endtime()*.
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
We consider that as a problem already solved by Earthscope,
the USGS, and global network operators in systems they
use for creating the modern data archive of the FDSN.  Custom
experimental data may need to utilize Earthscope tools to fix problems not
covered by MsPASS.

Gap Processing
~~~~~~~~~~~~~~~~~~
Internally MsPASS handles data gaps with a subclass of the
:code:`TimeSeries` called :code:`TimeSeriesWGaps`.   That extension of
:code:`TimeSeries` is written in C++ and is documented
`here <https://www.mspass.org/cxx_api/mspass.html#mspass-namespace>`__.
Like :code:`TimeSeries` this class has python bindings created
with pybind11.  All the methods described in the C++ documentation
page have python bindings.  There are methods for defining gaps,
zeroing data in defined gaps, and deleting gaps.
See the doxygen pages linked above for details.
The python functions that currently deal with data gaps have a second
strategy for handling his problem best described in the context
of those functions.


Merging Data Segments
~~~~~~~~~~~~~~~~~~~~~~~~
There are currently two different methods in MsPASS to handle merging
continuous data segments:  (1) a special, implicit option of the
:py:meth:`mspasspy.db.database.Database.read_data` method of the
:py:class:`mspasspy.db.database.Database` class, and (2) the
processing function :py:func:`mspasspy.algorithms.window.merge`.
In addition, there is a special reader function called
:py:func:`mspasspy.db.ensembles.TimeIntervalReader` that can be used
to read fixed time windows of data.  That function uses :code:`merge`
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
a second issue is, however, variable.  Each miniseed packet has a time
tag that is comparable to the `t0` attribute of a :class:`TimeSeries` object
and end time field equivalent to the output of the :class:`TimeSeries`
endtime method.   If the `t0` value of a packet is greater than some
fractional tolerance of 1 sample more than the endtime of the previous
packet, a reader will invoke a gap handler.  A reader's gap handler
commonly has options for what to do with different kinds of "gaps", but
for this section our definition is defined by the way obspy
handles this problem with their :class:`Stream` merge method described
`here <https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream.merge.html>`__.
That particular algorithm is invoked when reading miniseed data
if and only if a block of data defined running the mspass
function :py:meth:`mspasspy.db.database.Database.index_mseed_file` is
run with the optional argument `segment_time_tears` is set False.
(Note the default is True.).   If you need to use this approach, you will
need to also take care in defining the value of the following arguments
that are passed to obspy's merge function for gap handle:
`merge_method`, `merge_fill_value`, and `merge_interpolation_samples`.
Those three arguments are passed directly to obspy merge arguments with
a variant of the same names:  `method`, `fill_value`, and `interpolation_samples`.

Note an alternative user's who have previously used obspy for this functionality
may want to consider is to write a custom function that utilizes obspy's merge
directly rather than the implied used in read_data.

MsPASS merge function
-----------------------
MsPASS has a native version of a function with a capability similar to
the obspy merge function noted above.  The MsPASS function add some additional
features and, although not verified by formal testing,
is likely much faster than the obpsy version due to fundamental differences
in the implementation.
The docstring for :py:func:`mspasspy.algorithms.window.merge` describes more
details but some key features of this function are:

- Like obspy's function of the same name its purpose is to glue/merge
  a set of waveform components into a single, continuous time series.
  A key difference is that the obspy function requires an obspy
  Stream object as input while the MsPASS function uses the "member"
  container of a :class:`TimeSeriesEnsemble` object as input.
- It provides for an optional windowing of the merged result.  That approach
  is useful, for example, for carving events out from a local archive of
  continuous waveform data in a single step. This feature is useful for
  reducing the memory footprint of a parallel job.
- Gaps are flagged and posted with a Metadata approach.  Obspy has a set of
  options for gap handling that are inseparable from the function.
  Any detected gaps in the
  MsPASS merge function are posted to the Metadata component of the
  :class:`TimeSeries` it returns accessible with the key "gaps".
  The content of the "gaps" attribute is a list of one or more
  python dictionaries with the keyworks "starttime" and "endtime"
  defining the epoch time range of all gaps in the returned datum.
  The function also has an optional "zero_gaps".  When set True
  (default is False) any gaps are explicitly set to zeros.   By default
  the values should be treated as undefined, although in practice they
  are likely zeros.
- Overlap handling is controlled by another boolean parameter
  with the name "fix_overlaps".   When set True the function will
  check for overlapping data and attempt to repair overlaps only if
  the sample numerical data match within machine tolerance.
  The default behavior is to mark the return dead if any overlap is
  detected.  Obspy uses a less dogmatic algorithm driven by an optional
  function argument called "interpolation_samples".  As noted above it has
  been our experience that, in general, overlapping data always indicate
  a data quality problem that invalidates the data when the samples
  do not match.  If you need
  the obspy functionality use the
  :py:func:`mspasspy.util.converter.TimeSeriesEnsemble2Stream` and the
  inverse :py:func:`mspasspy.util.converter.Trace2TimeSeriesEnsemble`
  to create the obspy input and then restore the returned data to
  the MsPASS internal data structures

TimeIntervalReader
-----------------------
A second MsPASS tool for working with continuous data is a function
with the descriptive name :py:func:`mspasspy.db.ensembles.TimeIntervalReader`.
It is designed to do the high-level task of cutting a fixed time
interval of data from one or more channels of a continuous data archive.
This function is built on top of the lower-level
:py:func:`mspasspy.algorithms.window.merge` but is best thought of as
an alternative reader to create ensembles cut from a continuous data archive.
For that reason the required arguments are a database handle and the
time interval of data to be extracted from the archive.  Gap and overlap
handling is handled by :code:`merge`.

Examples
------------
*Example 1:  Create a single waveform in a defined time window
from continuous data archive.*
This script will create a longer :class:`TimeSeries` object from a set day files
for the BHZ channel of GSN station AAK.   Ranges are constant for a simple
illustration:

.. code-block:: python

    # code above would define database handle db
    from mspasspy.algorithms.window import merge
    from obspy import UTCDateTime
    from bson import json_utils  #TODO  verify this is right
    net ="II"
    sta="AAK"
    chan="BHZ"
    loc="00"    # STS-1 sensor at AAK
    # TODO:   select a reasonable time interval
    output_stime=UTCDateTime()
    output_etime=UTCDateTime()
    # this is a MongoDB query to retrieve all segments with data in the
    # desired time range of output_stime to output_etime
    query = {
      "$and": [
        { "sta" : {"$eq" : sta}},
        { "net" : {"$eq" : net}},
        { "chan" : {"$eq" : chan}},
        { "loc" : {"$eq" : loc}},
        { "starttime" : {"$lte" : output_etime}},
        { "endtime" : {"$gte" : output_stime}}
      ]
    }
    cursor=db.wf_miniseed.find(query).sort()   # TODO work out sort format
    tsens = db.read_data(query,collection="wf_miniseed")
    if tsens.live:
      merged_data = merge(tsens.member,output_starttime,output_endtime)
      if merged_data.live:
        print("Output is ok and has ",merged_data.npts," data samples")
      else:
        print("Data have problems - gaps or overlaps caused the datum to be killed")
    else:
      print("The following query yielded no data:")
      print(json.utils.dumps(query,indent=2))

*Example 2: parallel read from continuous archive*  This example is a workflow
to build a dataset of waveforms
segmented around a set of previously measured P wave arrival time from
an archive of continuous data.   The example is not complete as it
requires implementing a custom function that below is given the symbolic
name "arrivals2list".  From that list we create a dask bag and use it
to drive a parallel read with `read_distributed_data` that passes a
series of enembles to a function defined at the top that runs `merge`.
The example is made up, but is a prototype for building an event-based
data set of all waveforms with P wave times packed the the
Earthscope Array Network Facility (ANF) available online
from Earthscope.

.. code-block:: python

    from mspasspy.db.DBClient import DBClient
    import dask.bag as dbg
    dbclient=DBClient()
    # we need two database handles.  One for the continuous data (dbc)
    # and one to save the segments  (dbo).
    dbc = dbclient.get_database("TA2010")   # TA continuous data from 2010
    dbo = dbclient.get_database("Pdata2010")  # arrivals from ANF picks

    def query_generator(doc):
      """
      Generates a MongoDB query to run against wf_miniseed for waveform
      segments containing any of the time interval time+stwin<=t<=time+etwin.
      Returns a python dict that is used by read_distributed_data to
      generate a dask bag of ensembles.  Note this is an illustrative example
      and makes no sanity checks on inputs for simplicity.

      The input is the same python dict later loaded with the data using
      the container_to_merge argument of read_distributed_data.
      """
      net = doc["net"]
      sta = doc["sta"]
      time = doc["arrival_time"]
      query["net"]=net
      query["sta"]=sta
      stime=time+stwin
      etime=time+etwin
      query = {
        "$and": [
          { "sta" : {"$eq" : sta}},
          { "net" : {"$eq" : net},
          { "starttime" : {"$lte" : etime}},
          { "endtime" : {"$gte" : stime}}
        ]
      }
      return query

    def make_segments(ensemble,stwin,etwin):
      """
      Function used in parallel map operator to create the main output of
      this example workflow.  The input is assumed to be a time-sorted ensemble
      with all data overlapping with the time window defined by
        stwin <= t-arrival_time <= etwin
      where t is time of a d data sample. i.e. stwin an etwin are times relative
      to the arrival time.   The input ensemble is assumed to normally
      contain multiple channels.  The algorithm works through all channels it
      finds.  For each group if the number of segments is 1 it simply uses
      the WindowData function.  If multiple segments are present it calls the
      MsPASS merge function with fix_overlaps set True and with the time
      window requested.  That will return a single waveform segment
      when possible.  If the merge fails that segment will be posted but
      marked dead.

      :param ensemble:  input ensemble for a single station normally containing
        multiple channels.
      :param stwin:  output window relative start time
      :param etwin:  output window relative end UTCDateTime
      """
      # handle dead (empty) ensembles cleanly returning a default constructed
      # datum dead by definition
      if ensemble.dead():
        return TimeSeriesEnsemble()
      ensout=TimeSeriesEnsemble()
      net = ensemble["net"]

      sta = ensemble["sta"]
      time = ensemble["arrival_time"]
      # the ensemble will usually contain multiple channels.  We have to
      # handle each independently
      chanset = set()
      for d in ensemble.member:
        chan = d["chan"]
        if loc in d:
          loc=d["loc"]
        else:
          loc=None
        chanset.add([chan,loc])
      for chan,loc in chanset:
        enstmp=TimeSeriesEnsemble()
        for d in ensemble.member:
          if d["chan"] == chan:
            if loc:
              if d.is_defined("loc"):
                if d["loc"] == loc:
                  enstmp.member.append((d))
        # enstmp now has only members match chan and loc - now we can run merge
        # if needed.
        if len(enstmp.member)>1:
          d = merge(enstmp.member,time+stwin,time+etwin,fix_overlaps=True)
          ensout.member.append(d)
        else:
          # above logic means this only happens if there is only one segment
          # in that case we can just use WindowData
          d = WindowData(enstmp.member,time+stwin,time+etwin)
          ensout.member.append(d)
      return ensout

    # This undefined function would read the arrival time data
    # stored in some external form and return a list of python dict
    # with the keys 'net', 'sta', and 'arrival_time' defined.
    arrival_list = arrival2list(args)
    sort_clause=[("chan", 1), ("time",1)]
    # This creates a bag from arrival_list that we can pass to the
    # reader for loading with the container_to_merge argument
    arrival_bag = dbg.from_sequence(arrival_list)
    window_start_time = -100.0   # time of window start relative to arrival
    window_end_time = 300.0   # time of window end relative to arrival
    mybag = dbg.from_sequence(arrival_list)
    mybag = mybag.map(query_generator,window_start_time,window_end_time)
    qlist=mybag.compute()
    # qlist now is a list of python dict defining queries.  These are
    # passed to the parallel reader  to create a bag of ensemble objects.
    mybag = read_distributed_data(qlist,
          dbc,
          collection="wf_miniseed",
          sort_clause=sort_clause,
          container_to_merge=arrival_bag,
        )
    mybag = mybag.map(make_segments)
    # note the output of this function, with default here, is a list of
    # objectids of the saved waveforms
    out_ids = write_distributed_data(mybag,dbo,collection="wf_TimeSeries")

The above example is complicated a bit as it is an example of a parallel
job.  The parallel IO feature of this example are important as this
example could run very slowly as a serial job driven my millions of picks
that exists for the problem it simulates - an Earthscope TA
continuous data archive being accessed
to assemble a data set of several million waveform segments built from the
ANF catalog.  It may be helpful to expand on the main steps of this algorithm:

1.  The first step assumes the existence of an undefined function with
    the name `arrival2list`.   For the prototype example given it could
    be driven by the CSS3.0 tables created by the Earthscope
    Array Network Facility (ANF).  That data can currently be found
    `here <https://anf.ucsd.edu/tools/events/>`__.  The actual implementation
    would need to select what picks to use and pull out a restricted set of
    attributes from the CSS3.0 tables creating a large list of tuples
    with each tuple containing:  ['net', 'sta', 'arrival_time'] values.
    Note that step can be done in a couple of lines with the pandas
    module but is omitted as that is not a unique solution.  (e.g. one
    could also accomplish the same thing with a MongoDB database 'arrival'
    collection with suitable content.)
2.  The `from_sequence` method of dask bag creates a bag from a list.
    In this case it becomes a bag of python dict containers.
    The map call that follows
    using the custom function defined earlier in the code box creates
    a bag of python dictionaries that define queries to MongoDB.  What
    the queries are designed to do is described in the docstring for that
    function.
3.  We call the compute method to actually create the list of queries
    that will drive the reader.   That approach assumes the size of that
    container is not overwhelming, which is likely a good assumption since
    the individual dict containers are of the order of 100 bytes each.
4.  The called to `read_distributed_data` defines the main parallel workflow.
    In this mode it reads a (large) series of ensembles driven by the
    input query list.  This usage creates a implicit parallel reader.
    Each instance creates a `TimeSeriesEnsemble` with all channels
    for a particular station that have waveforms that intersect with the
    desired output time segment around the specified arrival time.
    An important feature exploited in the reader here is that implemented
    with the argument `container_to_merge`.  The docstrings give details
    but the main functionality it provides is a way to do a one-to-one
    mapping of a list of metadata loaded to the ensembles.  That feature
    adds a major efficiency for large data sets compared to the alternative of
    millions of MongoDB queries that one might consider to solve that problem.
    This example also requires the `sort_clause` argument to assure the
    queries return data in an order consistent with the requirements of the
    `make_segments` function that does all main work here.
5.  The map call following `read_distributed_data` calls the function
    earlier that handles the slice and dice operation.  How that is done is
    best gleaned fromt he docstring comments.
6.  This example calls the parallel writer, `write_distributed_data`, to
    save the results.

*Example 3:  Application of TimeIntervalReader.*
This example assumes we have a list of shot times from something like an
onshore-offshore experiment using airguns or a set set of land shots with
known shot times.  The script is serial, but is readily converted to
a parallel form using standard concepts described elsewhere in this user's manual.

.. code-block:: python

    from mspasspy.db.DBClient import DBClient
    import os
    dbclient=DBClient()
    db = dbclient.get_database("my_continuous_dataset")
    wstime=0.0
    wetime=50.0   # cut 50 s listen windows
    with fd = os.fopen("shottimes.txt"):
      lines = fd.readlines()
      for t in lines:
        tslist = TimeIntervalReader(db,t+wstime,t+wetime,fix_overlaps=True)
        for ts in tslist:
          db.save_data(ts)   # defaults to saving to wf_TimeSeries so omit data_tag
