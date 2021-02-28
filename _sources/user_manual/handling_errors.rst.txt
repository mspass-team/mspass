.. _handling_errors:

Handling Errors
===================

Overview
~~~~~~~~~~~~~~
Errors are a universal issue in large scale data processing.
In single threaded applications it was common to follow the classic unix
model of writing error messages to stderr.  Anyone processing large
amounts of data that can generate hundreds to thousands of error messages
know that stderr logging is problematic because the critical errors
can be hard to find in the debris.  A common solution used for single
threading was to use a log file that could be searched post mortem to
find problems.   In a multi-threaded situation that solution can work, but
requires specialized io libraries to avoid write collisions if two threads
try to write the to same file.  An example that may be familiar to many
seismologists of that approach is the elog library used in
`Antelope <https://brtt.com>`__.  That approach can work for a shared memory
system but is more problematic in a large cluster of distributed machines
or (worse) a cloud system.  In a distributed system a centralized error
logger would need to maintain a connection to every running process, which
would to a challenge to implement.

The above issues led us to a completely different solution for error logging.
We include an ErrorLogger class as a base class to all supported data
objects.  That model simplifies the error handling because the error
handling model can be stated as two simple rules:

1.  If there is any error that the user needs to be warned about, post
    an informative message to the error log.  Error are not always black
    and white so the message should define the severity level of the error.
2.  If the error invalidates the data, mark the data dead.  We expand
    on this concept further below in the section titled "Kills".

This section is organized around three key concepts used in the
error handling system of MsPASS:   standardized exception handling,
the error logger api, and concepts related to marking data dead.  The
last item has two important subtopics:  the kill concept and
special class we tag with the descriptive, albeit perhaps overly cute,
name of :code:`Undertaker`.

Exceptions and Error Handlers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Any book on programming in any language will address the issue of error handling.
The reason, of course, is that most algorithms have limitations on
what they receive as inputs or can evolve to a state that is untenable.
There are multiple standard ways to handle errors.   In MsPASS we aimed to
treat errors with one of three approaches.

1.  The python language is flexible, but standard practice encourages the
    use of the try-raise-except construct (the try-throw-catch keywords in C++).
    We use that exception mechanism internally to handle most error conditions.
    To provide some regularity any error raised by a MsPASS function or
    method will be the exception class called MsPASSError.   Following
    standard practice the MsPASSError is defined as a subclass of the
    base class Exception.  That allows generic error handlers like the
    following to catch MsPASSError exceptions as described in most
    books and online python tutorials
    (`Follow this link for a good example <https://medium.com/better-programming/a-comprehensive-guide-to-handling-exceptions-in-python-7175f0ce81f7>`__)

2.  As with most python packages, with some algorithms we sometimes return
    something that can indicate full or partial success.   We reserve that
    model to situations where "partial success" is not really an error but
    the result of an algorithm that is expected to behave in different ways
    with different inputs.  For example, the save_inventory method of
    `database <../python_api/mspasspy.db.html#module-mspasspy.db.database>`__
    returns a tuple with three numbers: number of site documents added,
    number of channel documents added, and the number of stations in the
    obspy Inventory object that the method received.  The result is dependent
    on history of what was saved previously because this particular algorithm
    tests the input to not add duplicates to the database.

3.  Processing functions in MsPASS that are properly designed for the
    framework MUST obey a very important rule:  a function to be run in
    parallel should never throw an exception, but use the error log and (optional)
    kill method described below.

Error Logger
~~~~~~~~~~~~~~

As discussed in the overview section above, handling errors in a parallel
environment presents special problems.  A key issue is that the schedulers
MsPASS supports (Spark and Dask) both will abort a job if any function
throws an unhandled exception.  For that reason, if a MsPASS processing
function ever throws an exception it is a bug.  Instead, we always aim
to handle errors with what we call our ErrorLogger and a classic approach
using a kill mechanism.

The atomic data objects of MsPASS, which we call TimeSeries and Seismogram,
are defined and implemented in C++.  (They are bound to python through
wrappers using a package called pybind11.)  Both atomic data objects
have a C++ class called `ErrorLogger <../_static/html/classmspass_1_1utility_1_1_error_logger.html>`__
as a public attribute we define with the symbol :code:`elog`.  That mechanism
allows processing functions to handle all exceptions without aborting
through constructs similar to the following pythonic pseudocode:

.. code-block:: python

    try:
        # code that may throw an error processing data object "d"
    except MsPASSError as merr:
        d.elog.log_error(merr)
        if merr.severity() == ErrorSeverity.Invalid:
            d.kill()
    except Exception as err:
        d.log.log_error('Unexcepted exception: more details','Invalid')
        d.kill()

The kill method is described further in the next section.  The key point
is that generic error handlers catch any exceptions and post message to
the ErrorLogger carried with the data in a container
with the symbolic name elog.   An error posted to the ErrorLogger
always contains two components:  (1) a (hopefully informative)
string that describes the error, and (2) a severity tag.   The
class description
describes the severity method that returns a special class called
ErrorSeverity.   ErrorSeverity is technically a binding to a C++ enum
class that defines a finite set of values that should be understood
as follows:

:code:`Fatal`: A serious error that should cause the job to abort.   This
is reserved only for completely unrecoverable errors such as a malloc error.

:code:`Invalid`: This error indicates the algorithm could not produce valid
data.  Data with Invalid errors will always be marked dead.

:code:`Suspect`:  Suspect is reserved for the condition where the datum
is not killed, but the algorithm that posted it wants to give a hint that
you should consider not using it for additional processing.  There are currently
no examples of this error being posted, but we include it in the api
to allow that option.

:code:`Complaint`:  A complaint is posted for an error that was handled and
fully corrected.   Complaints are posted largely as informational messages
to warn the user there were problems with data they may want to correct
to avoid problems with different downstream algorithms.  Such errors can
also often be useful in determining why a later stage of processing aborts.

:code:`Debug`:  Reserved for verbose logging to track a problem.  Useful to
insert in a long running job where something is going wrong that is
yielding invalid data but the job is not aborting or logging errors that
define the problem.

:code:`Informative`:  Used for very verbose options on some algorithms to
extract some auxiliary information needed for some other purpose.

A final point about error logs is to how they are preserved.  Error
messages should always be examined after a processing sequence is completed
to appraise the validity of the result.  With a large data set is it is
very easy to generate huge error logs.  To make the result more manageable
all save operators automatically write any error log entries to
a special collection in MongoDB we call the :code:`elog` collection.

Kills
~~~~~~~~~
The approach of marking a piece of seismic data bad/dead is familiar to
anyone who has ever done seismic reflection processing.  All seismic
processing systems have a set of trace editing functions to mark
bad data.  That approach goes back to the earliest days of seismic reflection
processing as evidenced by a trace id field (technically an
unsigned int16) in SEGY that when set to a particular value (2) defines
that datum as dead.

The kill concept is useful in the MsPASS framework as a way to simplify
parallel workflows.  Spark and Dask both use a mechanism to abstract
an entire data set as a single container (called an RDD in Spark and a "Bag"
in Dask).  As described in detail in the section of this manual on
parallel processing, the model used by MsPASS assumes a processing function
to run in parallel applies the same function to every member of the dataset
defined by the RDD or Bag container.  The kill mechanism is a simple
mechanism to define data that should be considered no longer valid.   All
properly designed python functions in MsPASS automatically do nothing if
data are marked dead leaving the dead data as elements of the RDD/Bag.

A final point is that if a job is expected to kill a large fraction of data
there is a point where it becomes more efficient to clear the dataset of
dead data.   That needs to be done with some care if one wishes to preserve
error log entries that document why a datum was killed.   The
:code:`Undertaker` class, which described in the next section was designed
to handle such issues.

Undertaker
~~~~~~~~~~~~~
The class name is a programming joke, but the name is descriptive;  its jobs
is to deal with dead data.  It has three basic methods that can be applied in
a serial job:

1.  The :code:`bury_the_dead` is the recommended method for most workflows
    to handle dead data.  It accepts TimeSeries, Seismogram, and ensembles of
    either as input.  It saves error logs and metadata for killed data to a
    special collection we call "graveyard".  (NOT YET IMPLEMENTED AND SUBJECT
    TO CHANGE).   For ensembles the function returns an edited version of the
    ensemble with the dead data removed.
2.  The :code:`cremate` method can only be applied to ensembles.  It
    clears the dead data members from an ensemble and returns a clean
    copy with he dead data removed.  We call it :code:`cremate` because
    all traces of the data vanish; neither the error log or any identifiers
    of the destroyed data will be retained.
3.  The :code:`bring_out_your_dead` method, other than being the best python programming
    joke ever, is more specialized.  It is only relevant for
    ensembles.  It returns two ensembles:  one with all the live and one
    with all the dead data.  That approach can be used, for example, in
    testing automatic editing code.  An interactive job to evaluate
    how well the editing worked could use this method.

For parallel workflows - NEEDS SOMETHING WHEN THAT IS FINISHED.
