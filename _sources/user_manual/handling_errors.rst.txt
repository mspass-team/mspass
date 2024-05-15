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
last item has two important subtopics:  the kill concept and
a special class we tag with the descriptive, albeit perhaps overly cute,
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
    To provide some regularity, most error raised by a MsPASS function or
    method will be the exception class called MsPASSError.
    (The exception is error that match standard python exception types.)  Following
    standard practice the MsPASSError is defined as a subclass of the
    base class Exception.  That allows generic error handlers like the
    following to catch MsPASSError exceptions as described in most
    books and online python tutorials
    (`Follow this link for a good example <https://medium.com/better-programming/a-comprehensive-guide-to-handling-exceptions-in-python-7175f0ce81f7>`__)

2.  Many algorithms are not black and white but need to return
    something that can indicate the degree of success.   We reserve that
    model to situations where partial success is not really an error but
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
function ever throws an exception it is a bug or an unrecoverable
error that does invalidate an entire run.  Instead, we always aim
to handle errors with what we call our ErrorLogger and a classic approach
using a kill mechanism.

All seismic data objects of MsPASS
are defined and implemented in C++.  (They are bound to python through
wrappers using a package called pybind11.)  All
have a C++ class called `ErrorLogger <../_static/html/classmspass_1_1utility_1_1_error_logger.html>`__
as a public attribute we define with the symbol :code:`elog`.
(The python bindings are also defined in this link:
:py:class:`ErrorLogger<mspasspy.ccore.utility.ErrorLogger>`. )
That mechanism
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
the :py:class:`ErrorLogger<mspasspy.ccore.utility.ErrorLogger>`
carried with the data in a container
with the symbolic name elog.   An error posted to the
:py:class:`ErrorLogger<mspasspy.ccore.utility.ErrorLogger>`
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
Concepts
----------
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

Since v2 of MsPASS the package defines two forms of dead dead:
(1) normal kills created during data processing and (2) what we
(colorfully) call "abortions".   The name is descriptive because a
dead datum marked as an "abortion" was never born.   The tag is
defined only during read operations if the construction of the object
fails.   Users should put aside any political views on the human
version of this topic and take a strictly pro life stance on
MsPASS abortions.  All abortions are a bad thing that should be
eliminated if they happen.   For the same reason MsPASS algorithms rarely
throw exceptions, when abortions occur (always during reading)
the :code:`Metadata` created from a MongoDB document are stored in
a dead datum with no sample data.   That body will be carried
through a workflow.   Abortions are handled specially by during a save
as described below.

A second feature added to MsPASS since v2 is that an entire ensemble
can be killed.  Killing an ensemble is equivalent to killing all the
atomic data members.

Handling dead data has two important, practical constraints:

#.  Any careful scientist will want to have a record of what data was
    killed and why it was killed.
#.  As noted earlier with a parallel container the body needs to be
    carried through the processing.   If the data objects are large
    moving the entire body around is inefficient and unnecessarily
    consumes memory.

How we address these two constraints is described in two sections
below.  The first is handled automatically by the
:py:meth:`save_data<mspasspy.db.database.Database.save_data>` method of
:py:class:`Database<mspasspy.db.database.Database>`.  The second has
options that are implemented as methods of the class
:py:class:`Undertaker<mspasspy.db.util.Undertaker.Undertaker>` that is the
topic of the second subsection below.

A final point is that if a job is expected to kill a large fraction of data
there is a point where it becomes more efficient to clear the dataset of
dead data.   That needs to be done with some care if one wishes to preserve
error log entries that document why a datum was killed.   The
:py:class:`Undertaker<mspasspy.db.util.Undertaker.Undertaker>`
class, which described in the next section was designed
to handle such issues.

Database handling of dead data
---------------------------------
The standard way in MsPASS to preserve a record of killed data is
implicit when the data are saved via the Database method
:py:meth:`save_data<mspasspy.db.database.Database.save_data>`.
The :py:class:`Database<mspasspy.db.database.Database>` class internally
creates an instance of
:py:class:`Undertaker<mspasspy.util.Undertaker.Undertaker>`
(Described in more detail the next section and the docstring
viewable via the above link.) that handles the dead data during the save
operation.  The
:py:meth:`save_data<mspasspy.db.database.Database.save_data>`
method has these features:

#.  If an atomic datum is marked dead,
    :py:meth:`save_data<mspasspy.db.database.Database.save_data>`
    calls the :py:meth:`bury<mspasspy.util.Undertaker.Undertaker.bury>`
    method of :py:class:`Undertaker<mspasspy.util.Undertaker.Undertaker>` on the
    contents.  The default behavior of
    :py:meth:`bury<mspasspy.util.Undertaker.Undertaker.bury>`
    is to create a document in the
    :code:`cemetery` collection with two primary key-value pairs:
    (a) The :code:`logdata` key is associated with a readable dump of the
    :py:class:`ErrorLogger<mspasspy.ccore.utility.ErrorLogger>` content.  (b) The :code:`tombstone` key is
    associated with a python dictionary (subdocument in MongoDB jargon)
    that is an image of the datum's :py:class:`Metadata<mspasspy.ccore.utility.Metadata>`
    container. If the :code:`return_data` boolean is set True (default is False),
    the sample vector/array will be cleared and set to zero length on the
    returned object.
#.  A special case is atomic data that are marked as "abortions".
    That property is marked by the reader by setting a Metadata boolean
    with the key
    :code:`is_abortion` to True.  The only difference
    in how these are handled is that "abortions"
    are saved to the :code:`abortions` collection instead of the
    :code:`cemetery` collection used for data killed during processing.
    The reason for that separation is to emphasize the pro-life
    stance of MsPASS - abortions should always be considered a serious error.
#.  Handling ensembles is a more complex problem because there are two
    very different definintions of dead:  (a) the entire ensemble can be
    marked dead or (b) only some members are marked dead.
    If the entire ensemble is marked dead, a common message is posted to
    all members and the :py:meth:`bury<mspasspy.util.Undertaker.Undertaker.bury>`
    method is called on all members.   If :code:`return_data` is
    set True, the member data vector is cleared.  In the more common
    situation where only some of the ensemble members are marked dead,
    :py:meth:`save_data<mspasspy.db.database.Database.save_data>`
    calls a special member of :py:class:`Undertaker<mspasspy.util.Undertaker.Undertaker>`
    with a name that is the best python joke ever:
    :py:meth:`bring_out_your_dead<mspasspy.util.Undertaker.Undertaker.bring_out_your_dead>`.
    The dead members are separated from those marked live and
    passed in a serial loop to :py:meth:`bury<mspasspy.util.Undertaker.Undertaker.bury>`.
    If :code:`return_data` is set True, the member vector is replaced with
    a smaller version with the dead removed.
#.  Saves of both atomic an ensemble data have a :code:`cremate` option.
    When set True dead data will be cleared without a trace.

Since V2 of MsPASS the recommended way to terminate a parallel
processing sequence is to use the mspass
:py:func:`write_distributed_data<mspasspy.io.distributed.write_distributed_data>` function.
It handles dead data the same way as
:py:meth:`save_data<mspasspy.db.database.Database.save_data>` described above.

Finally, users are warned that data that are passed through a reduce operator
will normally discard dead data with no trace.  If your workflow has a
reduce operator, it is recommended that you use the inline methods for
handling dead data described in the next section immediately before
the reduce operator is called.

Handling Dead Data as an Intermediate Step
---------------------------------------------
If your workflow has edit procedures that kill a significant fraction
of your dataset, you should consider using the MsPASS
facility for handling dead data within a processing sequence.
The main tool for doing so are methods of the
:py:class:`Undertaker<mspasspy.util.Undertaker.Undertaker>` class.
The class name is a programming joke, but the name is descriptive;  its job
is to deal with dead data.  The class interacts with a Database and has
three methods that are most useful for any MsPASS workflow.

1.  The :py:meth:`bury<mspasspy.util.Undertaker.Undertaker.bury>` method
    is the normal tool of choice to handle dead data.   It has the
    behavior noted above creating a document in the :code:`cemetery`
    collection for every dead datum.  For atomic data the return
    is a copy of the input object with the sample data cleared and
    the objects :code:`npts` attribute is set to 0.   For ensembles,
    this method returns a copy of the ensemble with the dead
    members completely removed.   A :code:`cemetery` document is
    saved for each datum removed from the ensemble.
2.  The :py:meth:`cremate<mspasspy.util.Undertaker.Undertaker.cremate>` method
    can be used if you do not want to preserve the error messages that
    caused kills.   With atomic data it returns the smallest ashes
    possible; a default constructed instance of the parent data object.
    For ensembles dead data are completely removed.
3.  The :py:meth:`bring_out_your_dead<mspasspy.util.Undertaker.Undertaker.bring_out_your_dead>` method,
    will raise an exception if it receives anything but an ensemble.
    It returns two ensembles:  one with all the live and one
    with all the dead data.  It is actually used internally by
    both the :py:meth:`bury<mspasspy.util.Undertaker.Undertaker.bury>`
    and :py:meth:`cremate<mspasspy.util.Undertaker.Undertaker.cremate>` methods
    when the input is an ensemble.
4.  :py:meth:`mummify<mspasspy.util.Undertaker.Undertaker.mummify>` is useful for
    reducing the memory footprint of a dataset while preserving the data
    that is normally saved in :code:`cemetery` at the end of a workflow.
    It does so by only clearing the sample data arrays and setting the
    :code:`npts` attribute for dead data to 0.   With ensembles the
    algorithm runs through all members clear the sample arrays of all
    members marked dead.

The following is a sketch of a typical use of an instance of
:py:class:`Undertaker<mspasspy.util.Undertaker.Undertaker>` within a workflow.
A key point is an instance of the class has to be instantiated
prior to the data processing workflow steps.

.. code-block:: python

  stedronsky = Undertaker(db)  # db is an instance of Database created earlier
  data = read_distributed_data(db,query)
  data = data.map(customfunction)  # some custom function that may kill
  data = data.map(squad)  # kills with an instance of the FiringSquad editor
  data.map(stedronsky.bury)
  # other processing commands would go here.
