.. _time_standard_constraints:

Time Standard Constraints
==========================
The :ref:`BasicTimeSeries discussion <data_object_design_concepts>` explains
the time model that allows MsPASS to support both active-source and passive
array data.  This section adds implementation details that matter when
constructing data manually, especially simulation data created for testing or
comparison with observations (traditional synthetic seismograms).

First, we will state the finite set of time issues the framework needs to
support:

1. Passive-array recordings can be assumed to use a global timing standard.
   Today that usually means GPS clocks, while older data used timing systems
   with lower precision.  We represent all such data with
   ``TimeReferenceType.UTC``.  Other global standards, such as raw GPS time,
   are assumed to be converted to UTC on import.  UTC-derived data can be in
   one of two states:

   a. Unshifted data have time 0 at the Unix epoch, 1970-01-01
      00:00:00 UTC.  A sample time is therefore a Unix epoch value in seconds.
      For ObsPy users, that value is identical to the ``timestamp`` property
      of :py:class:`UTCDateTime<obspy.core.utcdatetime.UTCDateTime>`.

   b. An algorithm can apply ``ator`` to shift time 0 to a specified epoch
      time.  Shifting by event origin converts the axis to travel time.
      :py:func:`ArrivalTimeReference<mspasspy.ccore.algorithms.basic.ArrivalTimeReference>`
      instead sets time 0 to a predicted or measured phase arrival.  Arrival
      time referencing is useful for visualization and for windowing the
      section of data relevant to a phase.  SAC users can view ``ator`` as a
      generalization of SAC's finite set of reference-time choices.

   Unshifted UTC data normally have the protected ``t0shift_is_valid`` flag
   set to ``False`` because no conversion shift is being stored.  Calling
   ``ator`` changes the time reference to ``Relative``, stores the supplied
   UTC reference time, and sets that flag to ``True`` so ``rtoa`` can reverse
   the conversion.  In Python, set or inspect the time standard with the
   ``tref`` property; the C++ ``set_tref`` method is intentionally not exposed
   as a Python method.

2. Active-source data are normally best loaded with the time reference set to
   ``TimeReferenceType.Relative`` in Python or
   ``TimeReferenceType::Relative`` in C++.  Relative time in this context is
   usually equivalent to having time 0 at the shot time.  MsPASS supports
   three broad levels of absolute-time precision in active-source data:

   a. Much older data had imprecise absolute timing or none at all.  Old SEGY
      files commonly omit the absolute-time fields because traditional
      reflection processing needs travel time, not absolute time; an absolute
      tag in that workflow is often useful only for acquisition bookkeeping.

   b. Newer seismic-reflection loggers generally have a system clock and write
      a date stamp, but that clock may not be accurate enough for a reliable
      absolute conversion.

   c. Many newer systems synchronize to GPS.  Data from these instruments are
      conceptually UTC data shifted with ``ator(shot_time)`` so time 0 is the
      shot time.  PASSCAL recorders that use GPS timing or GPS-calibrated
      internal clocks are a common example.

   In the first two cases, no UTC conversion shift should be installed;
   ``shifted()`` should return ``False`` and ``rtoa`` will reject the
   conversion.  A custom Python reader can set
   ``d.tref = TimeReferenceType.Relative`` on a newly constructed object, but
   changing ``tref`` alone does not create, clear, or validate a stored shift.

3. Synthetic seismograms require careful handling because most generators use
   a Relative time standard with 0 at source origin.  Matching synthetics to
   observations may require shifting the UTC observations to relative time or
   converting synthetics to UTC.  Shifting observations requires only
   ``ator``.  Converting a synthetic to UTC requires explicitly installing a
   valid origin-time reference before calling ``rtoa``, as shown below.

With that background, the time API can be summarized as follows.  These are
components of the C++ ``BasicTimeSeries`` base class.  Python objects such as
:py:class:`TimeSeries <mspasspy.ccore.seismic.TimeSeries>` and
:py:class:`Seismogram <mspasspy.ccore.seismic.Seismogram>` inherit the same
interface, with a few deliberate binding differences.

* C++ provides the ``t0()`` getter and ``set_t0()`` setter.  Python exposes
  the same operations through the ``t0`` property:

  .. code-block:: python

     d.t0 = arrival_time  # setter; calls the C++ set_t0 implementation
     t = d.t0             # getter; returns the C++ t0 value

* ``time(i)`` returns the computed time of sample ``i`` in the current time
  standard.  ``endtime()`` returns ``time(npts - 1)``, and
  ``sample_number(t)`` performs the inverse calculation rounded to the nearest
  sample.  These calculations do not enforce bounds: ``time(i)`` can evaluate
  an index outside the stored data, and ``sample_number(t)`` can return one.
  Check ``0 <= i < d.npts`` before using an index to access samples.
  In C++, ``time_axis()`` returns an ``std::vector<double>`` of length
  ``npts`` containing ``time(i)`` for every sample.  The Python binding
  exposes that vector as a ``DoubleVector``, which is useful as a plotting
  axis.

* C++ exposes ``set_tref(TimeReferenceType)``, but Python intentionally omits
  that method and provides a read/write ``tref`` property instead:

  .. code-block:: python

     from mspasspy.ccore.seismic import TimeReferenceType

     d.tref = TimeReferenceType.Relative
     if d.tref == TimeReferenceType.Relative:
         print("relative time")

  Setting ``tref`` changes only the reference-type enum.  It does not alter
  ``t0`` or the stored conversion shift.

* ``ator(reference_time)`` converts a live UTC datum to relative time by
  subtracting the supplied epoch time from ``t0`` and storing that epoch time
  for reversal.  ``rtoa()`` uses the stored shift to restore UTC, then clears
  it.  Both methods leave dead data unchanged; ``ator`` is also a no-op for
  data already marked Relative, and ``rtoa`` is a no-op for data already
  marked UTC.  ObsPy's ``UTCDateTime.timestamp`` is a float property, not a
  method:

  .. code-block:: python

     from obspy import UTCDateTime
     from mspasspy.ccore.seismic import TimeReferenceType, TimeSeries

     arrival_time = UTCDateTime("2020-01-01T00:00:00").timestamp
     d = TimeSeries(3)
     d.live = True
     d.dt = 1.0
     d.t0 = arrival_time - 1.0
     d.tref = TimeReferenceType.UTC

     d.ator(arrival_time)
     assert d.tref == TimeReferenceType.Relative
     assert d.t0 == -1.0
     assert d.shifted()

     d.rtoa()
     assert d.tref == TimeReferenceType.UTC
     assert d.t0 == arrival_time - 1.0
     assert not d.shifted()

  ``shift(delta_time)`` changes the stored reference time from ``r`` to
  ``r + delta_time`` and adjusts relative ``t0`` accordingly.  It is intended
  for live relative data with a valid stored shift, normally produced by
  ``ator``.  It is useful for switching between two relative references, such
  as an origin time and a phase-arrival time.  On live relative data without
  a valid shift, and absent the legacy fallback described below, it raises
  ``MsPASSError``.

* The protected C++ flag ``t0shift_is_valid`` is exposed through
  ``shifted()``: the method returns that flag's value exactly.  By convention,
  ``True`` means that a stored UTC conversion shift is available.  The method
  does not independently validate the value installed by ``force_t0_shift``.
  It is normally ``False`` for unshifted UTC data and for relative
  active-source data without reliable absolute timing.
  ``time_reference()`` returns the stored reference only for relative data
  with a valid shift; it raises an ``MsPASSError`` for UTC data or an invalid
  relative reference.  ``get_t0shift()`` returns the raw stored value without
  those consistency checks.  The current C++ ``rtoa`` retains a legacy
  fallback that accepts an unflagged raw shift greater than 100 seconds, but
  new code should not construct or rely on that inconsistent state.

* ``force_t0_shift(reference_time)`` is an expert-level escape hatch.  It
  stores the supplied conversion shift and makes ``shifted()`` true, but it
  does **not** change ``t0`` or ``tref``.  For example, a live synthetic whose
  existing ``t0`` is relative to origin time can be converted to UTC as
  follows:

  .. code-block:: python

     from obspy import UTCDateTime
     from mspasspy.ccore.seismic import TimeReferenceType, TimeSeries

     synthetic = TimeSeries(3)
     synthetic.live = True
     synthetic.dt = 1.0
     synthetic.t0 = -1.0
     synthetic.tref = TimeReferenceType.Relative

     origin_time = UTCDateTime("2020-01-01T00:00:00").timestamp
     synthetic.force_t0_shift(origin_time)
     assert synthetic.t0 == -1.0  # force_t0_shift did not alter the axis
     synthetic.rtoa()

     assert synthetic.tref == TimeReferenceType.UTC
     assert synthetic.t0 == origin_time - 1.0

  Do not call ``force_t0_shift`` for relative data whose absolute reference is
  unknown or unreliable; doing so bypasses the safeguard that makes ``rtoa``
  reject that conversion.
