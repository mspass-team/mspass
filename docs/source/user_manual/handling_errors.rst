.. _handling_errors:

Handling Errors
===============

Overview
~~~~~~~~
Errors are unavoidable in large-scale data processing.  A single-process
program can write messages to standard error or to one log file, but either
approach becomes awkward when hundreds of workers can report thousands of
messages.  Shared log files need coordinated I/O to avoid write collisions,
and a central logger for a distributed cluster or cloud deployment must
maintain communication with every worker.  The ``elog`` library used by
`Antelope <https://brtt.com>`__ is a familiar shared-memory example, but a
single large log also makes the few important failures hard to find.  Python's
standard logging framework does not by itself solve that per-datum scaling and
review problem.

MsPASS instead attaches an
:py:class:`ErrorLogger <mspasspy.ccore.utility.ErrorLogger>` to every seismic
data object.  The model has two simple rules:

1. If a user should know about a problem, add a useful message and severity
   to the datum's error log.
2. If the problem makes the datum unusable, mark that datum dead.  The
   :ref:`Kills <kills>` section develops this rule further.

The rest of this page covers three related concepts: standardized exceptions,
the error-logger API, and dead-data handling.  The last topic includes both the
kill convention and the descriptively named
:py:class:`Undertaker <mspasspy.util.Undertaker.Undertaker>` utility.

Exceptions and Error Handlers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Algorithms can reject invalid arguments, encounter a system failure, or
produce only a partial result.  MsPASS uses three corresponding patterns:

1. Python code uses ``try``/``raise``/``except`` and C++ uses
   ``try``/``throw``/``catch``.  Most library-specific failures use
   :py:exc:`MsPASSError <mspasspy.ccore.utility.MsPASSError>`, which is a
   subclass of Python's ``Exception``.  Normal Python exceptions such as
   ``TypeError`` and ``ValueError`` are still used for conventional argument
   and usage errors.  A caught ``MsPASSError`` exposes ``message`` and
   ``severity`` properties.

2. Some operations report partial success in their return value.  For example,
   :py:meth:`Database.save_inventory
   <mspasspy.db.database.Database.save_inventory>` avoids inserting duplicate
   station metadata and returns four counters: site documents saved, channel
   documents saved, distinct sites processed, and distinct channels processed.
   Those counts depend on what the database already contains; that variation is
   a result, not an exception.

3. A function mapped across a distributed dataset should normally keep a
   datum-local failure from escaping the worker.  It should log the problem,
   kill the affected datum when the result is invalid, and return that datum.
   Fatal, configuration, and system-level failures that invalidate the whole
   operation should still propagate.

The standard :py:func:`mspass_func_wrapper
<mspasspy.util.decorators.mspass_func_wrapper>` and
:py:func:`mspass_method_wrapper
<mspasspy.util.decorators.mspass_method_wrapper>` decorators implement a
conservative version of the third rule.  They normally return already-dead
inputs unchanged.  During execution they log ``RuntimeError`` and generic
exceptions as ``Invalid``; they also log any non-``Fatal`` ``MsPASSError``.
Those handled paths kill and return the input, while a ``Fatal``
``MsPASSError`` is re-raised.  Other decorators have policies appropriate to
their data flow; in particular, consult the
:py:func:`mspass_reduce_func_wrapper
<mspasspy.util.decorators.mspass_reduce_func_wrapper>` documentation rather
than assuming map-wrapper behavior for a reduce operation.

Error Logger
~~~~~~~~~~~~

An unhandled exception in code submitted to Spark or Dask fails the task and
can fail the job.  Datum-local problems should therefore be represented on the
datum whenever processing can safely continue; genuinely fatal failures should
remain exceptions.

MsPASS seismic data objects are implemented in C++ and exposed to Python with
pybind11.  Each has a public ``elog`` attribute containing the C++
`ErrorLogger <../_static/html/classmspass_1_1utility_1_1_error_logger.html>`__,
also exposed as
:py:class:`ErrorLogger <mspasspy.ccore.utility.ErrorLogger>`.  A manual handler
can use it as follows.  Standard decorators implement the more conservative
policy described above, so most decorated map functions do not need to repeat
this code.

.. code-block:: python

    from mspasspy.ccore.utility import ErrorSeverity, MsPASSError

    def process_one(d, algorithm):
        """Apply algorithm while localizing recoverable failures to d."""
        if d.dead():
            return d
        try:
            return algorithm(d)
        except MsPASSError as err:
            if err.severity == ErrorSeverity.Fatal:
                raise
            d.elog.log_error(algorithm.__name__, err.message, err.severity)
            if err.severity == ErrorSeverity.Invalid:
                d.kill()
            return d
        except Exception as err:
            d.elog.log_error(
                algorithm.__name__,
                f"Unexpected exception: {err}",
                ErrorSeverity.Invalid,
            )
            d.kill()
            return d

The broad ``Exception`` branch assumes that ``algorithm`` contains only
datum-local work.  Keep setup and whole-job system operations outside that
handler, or translate an unrecoverable condition to a ``Fatal``
``MsPASSError``, so it is not mistaken for one bad datum.

``ErrorLogger.log_error`` also accepts a caught ``MsPASSError`` directly.
The three-argument overload shown above records the algorithm name, message,
and :py:class:`ErrorSeverity <mspasspy.ccore.utility.ErrorSeverity>`, the
Python binding of the corresponding C++ enum.  Each stored ``LogData`` record
also carries the logger's job identifier and the worker process identifier.
The six severity values are:

``Fatal``
    The operation cannot safely continue.  A ``Fatal`` exception should be
    re-raised so the task or job can stop.

``Invalid``
    The algorithm could not produce a usable result.  Code that posts this
    severity should also kill the affected datum; merely adding an error-log
    entry does not change its live/dead state.

``Suspect``
    The datum remains usable, but the result is questionable and should be
    reviewed before further processing.

``Complaint``
    A problem was handled or corrected.  The datum remains live, but the
    message can explain later behavior or identify input that should be fixed.

``Debug``
    Diagnostic detail intended to help trace a problem in a running workflow.

``Informational``
    Auxiliary information rather than an error.  ``ErrorLogger.log_verbose``
    is a convenience method that posts this severity.

The enum value is descriptive; it does not itself kill data or raise an
exception.  If a ``Complaint`` or ``Suspect`` condition should leave a datum
live, post it directly to ``elog`` instead of throwing it through the
conservative map wrappers, which kill on every handled non-``Fatal``
``MsPASSError``.

Error messages should be reviewed after processing.  For a live atomic datum,
and for each live atomic member of an ensemble,
:py:meth:`Database.save_data <mspasspy.db.database.Database.save_data>` writes
a nonempty log to the MongoDB ``elog`` collection.  By default, dead atomic
data use the ``cemetery`` or ``abortions`` collections described below.  The
distributed writer uses ``elog`` by default for live atomic data, but its
``post_elog=True`` option embeds the log in the waveform document instead.
These queryable records keep large workflows from collapsing all diagnostics
into one oversized text file.

.. _kills:

Kills
~~~~~
Concepts
--------
The approach of marking a piece of seismic data bad/dead is familiar to
anyone who has ever done seismic reflection processing.  All seismic
processing systems have a set of trace editing functions to mark
bad data.  That approach goes back to the earliest days of seismic reflection
processing.  For example, a SEGY trace-identification code of 2 marks a dead
trace.

The kill concept is useful in the MsPASS framework as a way to simplify
parallel workflows.  Spark and Dask both use a mechanism to abstract
an entire data set as a single container (called an RDD in Spark and a "Bag"
in Dask).  As described in detail in the section of this manual on
parallel processing, the model used by MsPASS assumes a processing function
to run in parallel applies the same function to every member of the dataset
defined by the RDD or Bag container.  The kill mechanism is a simple
mechanism to define data that should no longer be used.  Many library
functions and the standard decorators return a dead datum without processing
it, leaving that object in the RDD or Bag until a later operation removes it.
Custom processing functions should follow the same convention.

Since version 2, MsPASS distinguishes two sources of dead data:

1. A normal kill occurs when processing determines that an existing datum is
   unusable.
2. An ``abortion`` is the API term for a datum that could not be constructed
   during a read.  Readers identify this case with the boolean Metadata key
   ``is_abortion=True``.  The available Metadata and error log are retained
   even though there are no usable samples.  Depending on the read path, that
   shell is either carried as a dead atomic datum or recorded immediately in
   the ``abortions`` collection and omitted from an ensemble.

An ensemble also has its own live/dead state.  Calling ``kill()`` on the
ensemble does not immediately rewrite every member's flag, but Undertaker
operations interpret a dead ensemble as meaning that all its members are dead.

Dead-data handling must satisfy two practical constraints:

#. Preserve a record of which data were killed and why.
#. Avoid carrying large sample arrays after those data become unusable.

The next section explains how
:py:meth:`Database.save_data <mspasspy.db.database.Database.save_data>` handles
the first requirement automatically.  The following section describes the
more granular options provided by
:py:class:`Undertaker <mspasspy.util.Undertaker.Undertaker>`.  When a workflow
kills a large fraction of its input, use those options to preserve the logs
before releasing sample storage or removing the dead objects.

Database handling of dead data
------------------------------
The standard way to preserve killed data is to let
:py:meth:`Database.save_data <mspasspy.db.database.Database.save_data>` handle
it during a save.  Every :py:class:`Database
<mspasspy.db.database.Database>` instance owns an
:py:class:`Undertaker <mspasspy.util.Undertaker.Undertaker>` for this purpose.
The save path has these features:

#. Unless ``cremate=True`` is requested, an atomic datum already marked dead
   when ``save_data`` begins is passed to
   :py:meth:`Undertaker.bury <mspasspy.util.Undertaker.Undertaker.bury>`.
   An ordinary kill creates a document in ``cemetery``.  Its main fields are
   ``logdata``, a list made from the datum's
   :py:class:`ErrorLogger <mspasspy.ccore.utility.ErrorLogger>`, and
   ``tombstone``, a dictionary copy of its
   :py:class:`Metadata <mspasspy.ccore.utility.Metadata>`.  An Undertaker
   constructed with a ``data_tag`` also stores that tag.  With
   ``save_data(return_data=True)``, the default burial path returns the dead
   datum with its sample array mummified to zero length.  ``return_data``
   belongs to ``save_data``; ``Undertaker.bury`` instead calls this option
   ``mummify_atomic_data``.

#. A datum marked ``is_abortion=True`` is written to ``abortions`` rather than
   ``cemetery``.  Its document contains a dictionary ``tombstone``, a ``type``
   identifying the intended data object, optional ``logdata``, and an optional
   ``data_tag``.  Keeping this category separate makes construction failures
   easy to query and repair.

#. For a live ensemble containing dead members, ``save_data`` uses
   :py:meth:`Undertaker.bring_out_your_dead
   <mspasspy.util.Undertaker.Undertaker.bring_out_your_dead>` to split the
   members, buries the dead ensemble member by member, and saves the live
   ensemble.  If the whole ensemble is dead, burial treats every member as
   dead and returns an empty ensemble shell.  With ``return_data=True``, the
   returned ensemble therefore has its dead members removed.  This process
   does not automatically add a common error message; algorithms that kill an
   ensemble remain responsible for logging the reason.  The save path persists
   member logs, not the ensemble container's ``elog``, so a reason that must be
   retained should be posted to the affected members.

#. ``save_data(..., cremate=True)`` deliberately suppresses persistence of
   dead data.  Treat this as a storage policy, not as a promise about the
   in-memory object returned when ``return_data=True``.  To obtain the explicit
   default-constructed "ashes" behavior described below, call
   ``Undertaker.cremate`` directly.

The recommended terminal writer for a parallel workflow is
:py:func:`write_distributed_data
<mspasspy.io.distributed.write_distributed_data>`.  It likewise buries dead
data by default.  With ``cremate=True`` it discards ordinary kills, while the
``Undertaker.cremate`` path still records abortions.

Finally, users are warned that data that are passed through a reduce operator
can be discarded with no output object on which to preserve their logs.  If a
workflow has a reduce stage, handle its dead inputs with the methods below
before invoking that stage.

Handling Dead Data as an Intermediate Step
-------------------------------------------
If editing kills a significant fraction of a dataset, handling the bodies
within the workflow can both preserve their diagnostics and reduce memory.
:py:class:`Undertaker <mspasspy.util.Undertaker.Undertaker>` provides four
principal methods:

1. :py:meth:`bury <mspasspy.util.Undertaker.Undertaker.bury>` persists each
   dead atomic datum to ``cemetery`` or ``abortions``.  For an ordinary dead
   atomic datum, the default mutates and returns that same object with ``npts``
   set to zero; setting ``mummify_atomic_data=False`` preserves its samples.
   An abortion is recorded and returned without that mummification step.  For
   an ensemble, ``bury`` returns a new ensemble containing only live members
   and buries each dead member.  A wholly dead ensemble produces an empty,
   dead ensemble.  A live atomic input is outside the method's intended atomic
   path and currently returns ``None``, so guard live atomic data before
   mapping this method.

2. :py:meth:`cremate <mspasspy.util.Undertaker.Undertaker.cremate>` returns a
   live atomic datum unchanged, returns a default-constructed object for an
   ordinary dead atomic datum, and removes dead members from an ensemble.
   Abortions are an exception: the method buries them before returning their
   ashes or removing them.

3. :py:meth:`bring_out_your_dead
   <mspasspy.util.Undertaker.Undertaker.bring_out_your_dead>` accepts only an
   ensemble and returns a two-element list ``[live_ensemble, dead_ensemble]``.
   It raises ``TypeError`` for an atomic object.  With its default
   ``bury=False`` it performs no database writes; callers can pass the second
   result explicitly to ``bury``.  ``Database.save_data`` uses this separation
   for live ensembles with dead members.

4. :py:meth:`mummify <mspasspy.util.Undertaker.Undertaker.mummify>` mutates
   dead atomic data in place by clearing the sample array.  By default it
   serializes the current error log into the datum's ``error_log`` Metadata
   field and clears ``elog``; ``post_history=True`` similarly embeds processing
   history.  For a live ensemble it mummifies only dead members, while a dead
   ensemble is treated as having all members dead.  This method does not write
   to MongoDB.

The following sketch applies burial safely to a Dask bag of atomic data before
a later reduce.  Construct the Undertaker before building the lazy processing
graph, pass ``query`` by keyword to ``read_distributed_data``, and retain the
new bag returned by every ``map`` call.  Map a ``FiringSquad`` instance's
``kill_if_true`` method rather than the instance itself because the inherited
``Executioner.__call__`` performs an in-place edit but returns ``None``.

.. code-block:: python

   from mspasspy.io.distributed import read_distributed_data
   from mspasspy.util.Undertaker import Undertaker

   undertaker = Undertaker(db)  # db is a Database created earlier
   data = read_distributed_data(db, query=query)
   data = data.map(custom_function)  # may kill an atomic datum
   data = data.map(squad.kill_if_true)  # squad is a FiringSquad instance

   def bury_if_dead(d):
       if d.live:
           return d
       return undertaker.bury(d)

   data = data.map(bury_if_dead)
   # Additional processing or a reduce operation follows.
