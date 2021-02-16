.. _time_standard_constraints:

Time Standard Constraints
==========================
|  The section in the user's manual on BasicTimeSeries concepts
   (This needs a link back to that section) discusses a novel feature of
   MsPASS that allows the framework to support both active source and
   passive array data.   This section contains additional implementation
   details that some users may find important.  The most notable example
   is simulation data created for testing or comparison with data
   (i.e. traditional synthetic seismogram concepts).

   First, we will state what we define as the finite set of time issues
   the framework needed to support:

   1.  The data for passive array recording can be assumed to be based on
       a global timing standard.  Today that usually means GPS clocks but
       older data used a variety of timing instrumentation that has evolved
       with time.   The only difference in older data is timing precision.
       We lump all such data by tagging them the :code:`TimeReferenceType` as :code:`UTC`.
       There are other global time standards like GPS time (such data do not
       include leap seconds) but we assume such data are converted to UTC
       on import.  Data derived from a UTC standard can be in one of two
       states:

       a.   Set with time 0 defined as UTC 0 time.   That is, the instant of
            New Year in 1970 (Jan. 1, 1970:00:00:00.0).   Time tag for a
            given sample (the output of the time method in BasicTimeSeries)
            is then a unix epoch seconds since epoch time 0.  Note for obpsy users that
            time is identical to the output of the timestamp method of UTCDateTime.
       b.   An algorithm can apply the ator method that will produce an
            internal shift of time 0 by a specified number of seconds.  Two
            choices of that shift are most common.  First, shifting by the
            origin of an event converts time to travel times.  Second,
            MsPASS has an algorithm called :code:`ArrivalTimeReference` that
            shifts data so time 0 is the arrival time of a particular phase.
            Note that time could be a predicted time from an earth model and
            hypocenter estimate or a time measured by picking or correlation
            method.  The arrival time reference is commonly used to
            improve plotting (visualization) of data or simply to reduce
            the volume of data to handle.   That is, algorithms aimed to
            process data around a specific section of data defined relative
            to a seismic phase nearly always window data using this time
            standard.  SAC users may understand the output of ator method
            as a generalization of the finite list of time standards supported
            in SAC.

       All data loaded originally with a UTC time standard have an internal
       boolean attribute (:code:`t0shift_is_valid`) set true to inform the
       system the data have a valid absolute time stamp.  Data loaded from
       seed files will always have this set properly, but custom readers for
       unusual formats must be aware of this constraint.  BasicTimeSeries
       time series has the method :code:`set_tref` that can be used to force the
       time standard.

   2.  Active source data are normally best loaded with the TimeReferenceType
       set as :code:`Relative` (TimeReferenceType.Relative in python or
       TimeReferenceType::Relative in C++).   Relative time in this context
       is nearly always equivalent to having time 0 set to the shot time.
        In the design of MsPASS, however, we aimed to support the range of
       instrumentation in active source data acquisition with three different
       absolute time precisions:

      (a)  Much old data had very imprecise absolute timing or none at all.
           It is common, for example, with old SEGY files to find none of
           the absolute fields defined. That is the case because in
           traditional active source processing like classic seismic reflection
           processing all that matters in processing is travel time.  In that case
           absolute time tags are baggage useful only for bookkeeping.
      (b)  Newer data seismic reflection data loggers always have a system
           clock that keeps time but not necessarily with high accuracy.
           These data loggers always write a date stamp to their internal file
           format, but it cannot be used to converted accurately to absolute
           time using that time stamp.
      (c)  Many of newer systems record time synchronized with GPS receivers.
           IRIS PASSCAL instruments beginning with the old SGR's through the
           most recent (at this writing) Nodal instruments depend upon accurate
           internal clocks.   Data from this type of instrumentation is
           conceptually identical to data that begins as UTC but is time shifted
           so TimeReferenceType is "Relative" and the time is shifted so 0
           is the shot time.

       The common denominator for all active source data is that the they should
       have TimeReferenceType set to "Relative".   The first two cases when
       the absolute timing is not reliable should have the internal attribute
       :code:`t0shift_is_valid` set to False.  That assures the users won't make the
       mistake of calling the rtoa function and creating a potentially misleading
       (at best) data set.  That constraint can be enforced using the :code:`set_tref`
       method that can be called from either TimeSeries or Seismogram objects.
       The complete set of methods available to handle this issue are discussed at the
       end of this document.
   3.  Synthetic seismograms are a special case that have to be handled with
       care.   The reason is putting synthetic seismogram signals into this
       framework almost always has to be carefully aware of the distinction
       between UTC and Relative time.  I know of now synthetic seismogram
       generator that internally uses anything other than the equivalent of
       a Relative time standard with 0 being source origin time.  Matching
       recorded signals, however, may require shifting either the data to
       relative time or shifting the synthetics to UTC time.  The two are
       completely equivalent but how they impact MsPASS is very different and
       must be considered.   Shifting data with a UTC time stamp to relative
       time is simplest - it involves only a call to ator.  The inverse
       requires some care with the synthetics as you would need to override the
       safeties described below.

|  With that background, here we summarize the API used to manage time.  All
   of these are components of the BasicTimeSeries base class, but there are
   some deviations between the C++ and python APIs.

   *  The time stamp that is to handle time 0 is managed in C++ by a getter
      with he simple name :code:`t0()` and a setter called :code:`set_t0()`.  In python
      these are merged to a single symbol t0 with decorators in a fairly
      standard way to make t0 appear like a class attribute as in the
      following:

      .. code-block:: python
         d.t0=arrival_time      # t0 as a setter (alias for set_to(arrival.time)
         t=d.t0                 # t0 as a getter (alias for to())

   *   There are several convenience methods that are useful for managing time
       as a variable.  The :code:`time(int i)` method can be used get the computed
       time (Relative or absolute depending on the TimeReferenceType set)
       of sample number i.   :code:`endtime()` is a special case that returns
       :code:`time(npts-1)` where npts is the number of points in the signal.  Finally,
       there is the inverse function :code:`sample_number(double t)` that returns
       the integer sample number computed for time t rounded to the nearest
       sample.   Users are cautioned that none of these methods check the
       validity of a time or sample number with respect to the data.  e.g. it
       would be ill-advised to use the output of :code:`sample_number(t)` as an
       index into the data array without checking the result is >0 and
       < npts.

   *   There are two methods to switch between UTC and Relative time standards.
       Use :code:`ator(time_shift)` to translate the time origin to the time
       defined by the epoch time :code:`time_shift`.   Note a convenient way to
       get such a time from a date string is the use obspy's UTCDateTime
       and apply the :code:`timestamp` method on the UTCDateTime object.  The
       inverse of :code:`ator` is :code:`rtoa()`.   Note the method has no arguments
       and uses the value of time_shift applied when :code:`ator` is called to
       restore time to UTC.  Finally, there is a :code:`shift(delta_time)`
       method intended to be used to tweek time 0.  That method should only
       be used if the data are in Relative time created by an earlier call
       to the ator method.  The main use of the :code:`shift` method is for things
       like switching between two relative time stamps (e.g. between two
       phase arrival times or between source origin time and a phase time).

   *   The problem of how to define if a relative time standard should be
       treated as reliable is managed internally by a private
       (technically protected in C++ but private from a python perspective),
       boolean attribute called :code:`t0shift_is_valid` in the C++ code.  That
       attribute can be interrogated with the method :code:`shifted()`.   The
       :code:`shifted` method returns true if the data are in UTC (not shifted
       by calling ator) OR they were never defined with respect to UTC
       (i.e. active source data like that noted above).  There are a
       (dangerous) pair of setter to force a time standard.
       First, it may be sometimes necessary to force the time standard
       with the method :code:`set_tref(rtype)` where rtype has the ugly form in python
       :code:`TimeReferenceType.UTC` or :code:`TimeReferenceType.Relative`.
       (At the risk of adding confusion the same symbols would be referred to
       as :code:`TimeReferenceType::UTC` and :code:`TimeReferenceType::Relative in C++)`.
       The second dangerous setter has the signature :code:`force_t0_shift(t)`.  It will
       set the data 0 value to t and set :code:`t0_shift_is_valid` to True.
       The primary purpose of these two methods is to match synthetics to
       data that are stored with UTC time.   By calling
       :code:`set_tref(TimeReferenceType.UTC)` followed by a call to :code:`force_t0_shift(t)`
       where t is the origin time of an event being simulated, a synthetic can
       be compared sample by sample to data.   
