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
would to a challenge to implement from scratch.   We originally considered
using python's logging mechanism, but recognized that would not solve the
large log file issue.

The above issues led us to a completely different solution for error logging.
We include an ErrorLogger class as a member of all supported data
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
last item has three important subtopics:  (1) the kill concept,
a special class of killed data we call "abortions", and (3)
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
    base class Exception.  That allows generic error handlers
    to catch MsPASSError exceptions as described in most
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
    parallel should never throw an exception, but use the error log and,
    where appropriate, the
    kill method described below.

Error Logger
~~~~~~~~~~~~~~

As discussed in the overview section above, handling errors in a parallel
environment presents special problems.  A key issue is that the schedulers
MsPASS supports (Spark and Dask) both will abort a job if any function
throws an unhandled exception.  For that reason, if a MsPASS processing
function ever throws an exception it is a bug or the problem was
created by an unresolvable issue unrealted to MsPASS.  Instead, we always aim
to handle errors with what we call our ErrorLogger and a classic approach
using a kill mechanism.

All of the data ojects of MsPASS,
which means `TimeSeries`, `Seismogram`, and ensembles of both atomic types,
are defined and implemented in C++.  (They are bound to python through
wrappers using a package called pybind11.)  All
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
with the symbolic name elog.   Note further than the standardization
makes it easier to write generic functions for handling multiple
MsPASS data objects.  An error posted to the ErrorLogger
always contains two components:  (1) a (hopefully informative)
string that describes the error, and (2) a severity tag.   The
class description
describes the severity method that returns a special class called
ErrorSeverity.   ErrorSeverity is technically a binding to a C++ enum
class that defines a finite set of values that should be understood
as follows:

:code:`Fatal`: A serious error that should cause the job to abort.   This
is reserved only for completely unrecoverable errors such as a malloc error.
When :code:`Fatal` errors are detected all current examples in MsPASS
throw a MsPASSError exception and do not continue.

:code:`Invalid`: This error indicates the algorithm could not produce valid
data.  Data with Invalid errors will always be marked dead.  Such errors,
however, do not throw an error but silently return a dead datum.

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

Abortions
~~~~~~~~~~~~
Early experience taught us that there was a special case of dead data
that needed to be treated separately.   That is, because the data objects
in MsPASS are made up of multiple components there are multiple ways
constructing one that is valid can fail.   Type examples that drove the
ideas recorded here are unrecoverable errors in data stored in the MongoDB
database that require a kill in the method of :code:`Database` called
:code:`read_data`.  Since version 2 of MsPASS we define a special case
for a dead datum we call an "abortion".   The following is a concise
statement of what that term means in the MsPASS context:

    An **abortion** is defined as a datum that is killed during construction.
    By definition it is invalid and the content is likely incomplete.

Abortions are distinguished internally by having two properties:
(1) they are by definition dead, and (2) the :code:`Metadata` attribute
"is_abortion" will be set to True
(the value associated with the key is a boolean type).
The MsPASS :code:`Dataase.read_data` and :code:`read_distributed_data`
functions may return such a datum.

During processing abortions are handled like all dead data, which means
they are largely ignored.  Specifically, all valid MsPASS processing functions
should silently return any dead datum doing nothing to it.
Abortions are handled differntly from data killed during processing in
only one way.  The body, so to speak, of abortions are saved in a special
database collection called "abortions" when a dataset is saved with
:code:`Database.save_data` or :code:`write_distributed_data`.
The structure of abortion documents that are saved are similar to
"normal" dead data discussed below.

We note a special case of abortions for ensembles.  That is, during
processing any ensemble often can contain a mix of live and dead atomic
data.   During a read, however, pervasive errors in database document
can create ensembles that have not live atomic members.  When that
happens the entire ensemble will be marked dead.

Undertaker
~~~~~~~~~~~~~
The class name is a programming joke, but the name is descriptive;  its jobs
is to provide a standard, socially acceptable way to handle dead data.
The class interacts with the MsPASS MongoDB database, which is why
a :code:`Database` is a required parameter for the constructor
which is worth showing to explain the concepts it implements:

.. code-block:: python

   class Undertaker:
   # docstring is here in actual implementation
   def __init__(self, dbin,
           regular_data_collection="cemetery",
           aborted_data_collection="abortions",
           data_tag=None,
           ):

As noted :code:`dbin` is expected to be an instance of
:class:`mspasspy.db.Database`.   As noted earlier in this section
we found it is useful to handle the special type of dead data we define
as "abortions" differently.  The constructor arguments show that by
default the bodies of data killed during processing will be saved in
the "cemetery" collection while abortions will be saved in a different
plot called "abortions".  (:code:`data_tag` is an optional parameter
used internally when this class is used inside the Database class
by :code:`save_data`.).

The class has a set of equally colorful names that define
four ways of handling dead data:

#.  :py:meth:`bury<mspasspy.util.Undertaker.bury>` will save the body
    of dead objects to either "cemetery" or "abortions" as a MongoDB
    document with the following primary attributes:

    * :code:`tombstone` will contain a subdocument (python dictionary)
      representation of the Metadata container of the dead datum.
    * :code:`logdata` will contain a python list of the ErrorLogger
      content of the dead datum.

    Note that when applied to atomic data one document will be produced
    in either "cemetery" or "abortions" for a dead datum.  When passed
    an ensemble one document will be produced for each dead ensemble
    member.   If the entire ensemble is marked dead, all members will be
    treated as dead even if they are individually marked live.
#.  :py:meth:`mummify<mspasspy.util.Undertaker.mummify>` is a method
    that is useful mainly for reducing memory use with a large dataset
    containing a significant fraction of dead data.   A "mummy"
    is defined as a datum with no sample data.   Hence, when applied to
    atomic data the sample array for
    a dead datum is cleared but the Metadata, history, and ErrorLogger
    containers are retained.  When run on ensembles the mummify
    method will be applied to dead members leaving the bodies in the
    ensemble.   If the entire ensemble is marked dead, all members will
    be mummified.

#.  :py:meth:`cremate<mspasspy.util.Undertaker.cremate` is an extreme
    version of mummify.  For atomic data it reduces the datum to the
    default constructed version of the data object, which has the minimum
    possible memory footprint.  It also, by definition, is a dead datum
    with no error log content.  When applied to ensembles dead memories will
    be completely removed.   If the ensemble is marked dead it will be default
    constructed which means the member vector will be empty.

Finally, there is the more specialized method called
:py:meth:`bring_out_your_dead<mspasspy.util.Undertaker.bring_out_your_dead>`.
In addition to being the best programming joke ever, user's need to recognize
this method only accept ensembles.  It is more-or-less a separator algorithm
returning two ensembles instead of one; one ensemble has only live members
and the other only the dead data. It is used internally, for example, in
:py:meth:`save_data<mspasspy.util.Undertaker.save_data>` to handle dead
data during a save.  There the live data are saved while the dead data are
passed through the :py:meth:`bury<mspasspy.util.Undertaker.bury>` method.
The most common application during processing is a method of memory
reduction with ensembles that saves the dead bodies.   The following code
segment shows how to accomplish that.

.. code-block:: python

   # Assume db is a Database handle defined in code above
   from mspasspy.util.Undertaker import Undertaker
   stedronsky = Undertaker(db)
   # example loading common source gathers
   srcid_list = db.wf_Seismogram.distinct("source_id")
   for srcid in srcid_list:
     query = {"source_id" : srcid}
     cursor = db.wf_Seismogram.find(query)
     ensemble = db.read_data(cursor,collection="wf_Seismogram")
     # processing code that kills data would go here
     [live_data, bodies] = stedronsky.bring_out_your_dead(ensemble)
     stedronsky.bury(bodies)
     del bodies
     # second stage processing code would go here

Note that the entire point of the above construct is to reduce memory
use for the second stage of processing following the application of
:py:meth:`bring_out_your_dead<mspasspy.util.Undertaker.bring_out_your_dead>`
while saving the bodies with call to
:py:meth:`bury<mspasspy.util.Undertaker.bury>`.
The :code:`del` statement is used to minimize memory use, but is
not essential.  The
