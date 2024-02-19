.. _data_editing:

Data Editing
=======================
Concepts
------------
MsPASS uses an idea that was been a part of seismic reflection from the
earliest days of digital processing of seismic signals commonly called
trace editing.   Trace editing means removing "bad" data from subsequent
processing where the meaning of "bad" is based on some computed metric or
human editing using a graphical user interface.   In all cases, the decision
of "good" or "bad" is not grey but a black and white (boolean) decision.
We implement this concept in MsPASS through a boolean `live` attribute
that is a component of all MsPASS data objects (i.e. both atomic data
TimeSeries and Seismogram as well as ensembles of same).   Any MsPASS
data object can be "killed" by calling it's `kill` method.   Algorithms
can and always should test the attribute `live` to verify the data object
has data that can be viewed as valid.  No algorithm should ever assume
data marked dead (live set False) can be used for any further processing.

In MsPASS we abstract the process of defining "trace editing" through
a standardized API.  In this section of the manual we describe how to
use the generic metrics that are part of MsPASS.   We also show how
users can add custom metrics by either computing custom Metadata or
running an inline computation on the sample data.

MsPASS Trace Editing Algorithms
----------------------------------

Simple Comparison operators - example for starters
+++++++++++++++++++++++++++++++++++++++++++++++++++++

All the algorithms currently implemented in MsPASS use another model
borrowed from seismic reflection processing.  That is, the editing is
driven by tests of the values of one or more "header" (As noted
elsewhere MsPASS data defined a generalized "header" we call Metadata)
attributes.   That means all standard numeric comparison operators
(in python the ``>``, ``<``, ``<=``, ``>=``, ``==``, and ``!=`` operators).

Before defining the abstraction and the common API it is, perhaps, more
instructive to give an example.   Suppose we wanted to exclude
all data from processing that have an estimated bandwidth less than 12 dB
(2 octaves).   The following is an example serial job looping over an entire
dataset defined by Seismogram objects:

.. code-block:: python

  # ... Preceding code: import commands and creating database handle db ...

  editor = MetadataLT("bandwidth",12.0)
  query = {}   # Change to a MongoDB query to process data subset
  cursor = db.wf_Seismogram.find(query)
  for doc in cursor:
    d = db.read_data(doc)
    d = arrival_snr_QC(d)
    d = editor(d)


We made this example as simple as possible by using all defaults for the
function ``arrival_snr_QC``.   That function has many options and depends upon
a detail that the example assumes was previously computed;  the Metadata attribute
"Parrival" is assumed to have been set to a time within the time interval
spanned by each datum.  The key step for the concepts of this section
is the creation of the python class
``MetadataLT`` in the first line.  The name is meant to be mnemonic for
using a Metadata test with the "Less Than (FORTRAN LT or python <)" operation
using the value stored in Metadata with the key "bandwidth".
We directly call the ``MetadataLT`` object, which is equivalent to calling the``kill_if_true`` method, to implement the test.
That method will return a version of ``d`` marked dead if the value of "bandwidth" is less than 12.0.

For completeness here is a parallel version of
that same script using dask:

.. code-block:: python

  editor = MetadataLT("bandwidth", 12.0)
  query = {}
  seisbag = db.read_distributed_data(db, query)
  seisbag = seisbag.map(arrival_snr_QC)
  seisbag = seisbag.map(editor)


where we also removed the comments and omit the (required to do anything)
call to ``seisbag.compute()`` because this example would always be
most appropriate as a section inside a workflow. 

All Simple Comparison Operators
+++++++++++++++++++++++++++++++++
All the simple comparison operators using tests against a Metadata single attribute
Have the same, common API with the following elements:

#. All operators are implemented as a subclass of an (abstract) base class called
   Executioner.
#. All operators have and one and only one constructor.
#. All the constructors of this set have have two required, positional argument.  The
   first is the Metadata key whose value is to compared.  The second (argument
   1 starting from 0) must contain the (constant) value for the comparison.
   In the example above the key
   is "bandwidth" and the test value is 12.0.
#. All operators have an optional ``verbose`` boolean argument.  When set True all kills
   will generate an informational error log entry with a detailed message
   on why the datum was killed.   The default, in all cases, verbose is False because
   trace editing of large data sets can produce bloated error logs
   when done in verbose mode.  We judged the possibility of filling the file system
   and crashing a bigger issue than losing a kill record.  Needless to say, if
   understanding reasons for why individual data are killed is important you
   should turn on verbose.
#. All operators have names of the form ``MetadataXX`` with ``Metadata`` a fixed string and
   XX the Fortranish name for the corresponding comparison operation.  For
   instance, in the example above we used ``MetadataLT`` which means the
   comparison used is the python ``<`` operator that FORTRAN would call ``.LT.``.
#. All operators implement the (required) method ``kill_if_true``.   As the name implies
   that method kills the datum if the conditional defined in construction of
   the object resolves true.   For our example above that means if ``x`` is the value
   extracted from Metadata and ``a`` is the boundary set as arg1 in the constructor,
   the datum is killed if ``x<a``.  This method always returns a copy of the
   data passed to it marked dead or alive depending on the outcome of the
   test.  That is essential for use of the function in a map operator like
   our parallel example above. All operators will throw a MsPASSError exception
   if the data received is not one of the data objects known to MsPASS.
   All implementations also silently do nothing if the input is already marked
   dead.
#. As a convenience the base class implements the ``__call__`` method.
   In this case the ``__call__`` method can be thought of as shorthand for the
   ``kill_if_true`` method.   We used this feature in the example above
   by using the form ``d = editor(d)`` that is a shorthand for the more
   explicit but verbose ``d = editor.kill_if_true(d)``.

The following is a table of the names of all the simple comparison functions
using the common API.   In each cell ``x`` is the value extracted from
Metadata and ``a`` is the boundary value for the comparison test.
We emphasize that in all cases the ``kill_if_true`` method, or the function
form, kills the datum if the test shown resolves as True.

.. list-table:: Simple Metadata-based Edit Classes
   :widths: 50 50
   :header-rows: 1

   * - Class name
     - Kill Test
   * - MetadataGT
     - x >  a
   * - MetadataGE
     - x >= a
   * - MetadataEQ
     - x == a
   * - MetadataNE
     - x != a
   * - MetadataLT
     - x < a
   * - MetadataLE
     - x <= a

The constructors for all simple comparison testers have this the following,
common signature:

.. code-block:: python

  def __init__(self, key, value, verbose=False):


where ``key`` is the Metadata key used to fetch the data for ``x`` in the
table above and `value` is the value assigned to ``a``.

The verbose flag is a common argument for all the MsPASS metadata-based
testers.   Normally (default ``verbose=False``) kills are done silently.
When set true all kills will generate an ``Informational`` elog entry with
a detailed message giving the details of why the datum was killed.

Existence Tests
++++++++++++++++++++++
Unlike classical header implementations that have fixed slots that
always have data in them, Metadata is open-ended.  That means data for a particular
key may or may not exist.   We thus supply two existence classes.
The class names are ``MetadataDefined`` and ``MetadataUndefined``.   The
``kill_if_true`` methods for these each kill a datum if a key loaded in
on construction exists or does not exist respectively.   Both
have constructors with this signature:

.. code-block:: python

  def __init__(self, key, verbose=False):


where ``key`` is the Metadata key that is to be tested by the kill_if_true
method.  verbose is as noted above for the simple comparison testers.

``MetadataUndefined`` is a particularly important editor to prefilter data
prior to running one or more processing functions.   If a function requires
one or more metadata attributes ``MetadataUndefined`` can be used to
filter out all data that would cause that algorithm to fail anyway.

Interval Comparison
++++++++++++++++++++++++

Another common test for editing data is filtering data defined by
a range of values.   A type example is P wave receiver functions that
commonly only use data with epicentral distances between about 30 and 100 degrees.
Another would be the size of some amplitude metric defined by a range of positive values.
A way to accomplish that within a workflow without using multiple simple
comparison operators is to apply an interval filter that
kills data outside a specified range.

There are two complications in defining a range test.  First, there are two
mirror-image tests:   is the value to be tested inside an interval or
outside the interval.
The second is should the test be inclusive of the edges?  i.e. should the
tests be ``<=`` or just ``<`` (similarly ``>=`` or ``>``)?   That could have been done with
nine different classes for all the possible combinations of the three boolean
variables it takes to define all the possibilities or a ``FiringSquad``
instance as describd below.  As a convenience we implemented
a single class called `MetadataInterval` with three boolean values defined
in the constructor.  The constructor has this signature:

.. code-block:: python

  def __init__(self, key, lower_endpoint, upper_endpoint,
    use_lower_edge=True, use_upper_edge=True, kill_if_outside=True, verbose=False):


The three booleans (``use_upper_edge``, ``use_lower_edge``, and ``kill_if_outside``)
determine how equality with the edges is handled and if the test is "inside" or
"outside" the specified range.  These are defined in the table below noting
that in the table ``a=lower_endpoint`` and ``b=upper_endpoint``.  In all cases
True means if the test is true the datum will be marked dead.

.. list-table:: MetadataInterval operators
   :widths: 30 30 30 30
   :header-rows: 1

   * - use_lower_edge
     - use_upper_edge
     - inside_test
     - Kill test
   * - True
     - True
     - True
     - a <= x <= b
   * - False
     - True
     - True
     - a < x <= b
   * - True
     - False
     - True
     - a <= x < b
   * - False
     - False
     - True
     - a < x < b
   * - True
     - True
     - False
     - x <= a and x >= b
   * - False
     - True
     - False
     - x < a and x >= b
   * - True
     - False
     - False
     - x <= a and x > b
   * - False
     - False
     - False
     - x < a and x > b



Defining Multiple Editors
++++++++++++++++++++++++++++++

The final Metadata-based editor in MsPASS was given the name ``FiringSquad``.
Although the name is admittedly a bit tongue-in-cheek, the imagery the name
provokes describes the function well:  a datum facing a firing squad
is facing multiple executioner who may or may not kill you.   This class
has a signature similar to the other Metadata-based editors:

.. code-block:: python

   class FiringSquad(Executioner):

Meaning it inherits the base class `Executioner` and requires a custom
implementation of the `kill_if_true` method. It can be used alone in
a workflow exactly like the single test functions described above.
A `FiringSquad` is simply a way to apply multiple Metadata tests
in a single function call to the `kill_if_true` method.

As with MetadataInterval
the constructor is different and has this signature:

.. code-block:: python

  def __init__(self, executioner_list, verbose=False):

where ``executioner_list`` is expected to be any iterable container made up
only of python classes that are subclasses of Executioner. (All the classes
covered in this document are subclasses of Executioner.)  Verbose has the
same meaning as described above with an important exception.  It is not
global but refers only to errors internal to ``FiringSquad``.  Any testers
needing a verbose option enabled will need to have the verbose option
specified during their construction.

When the ``kill_if_true`` method is called for this class the list of
executioners are called in order defined by the list.  The victim cannot
be hit by more than one bullet.  Once a datum is killed the ``kill_if_true``
method returns the body and drops further tests.

A feature of a ``FiringSquad`` not enabled in any of the other classes described
in this document is that it implements operator ``+=``.  The += operator
can be used is to append an
additional test to an existing ``FiringSquad``.   e.g. suppose we had a workflow
that creates a ``FiringSquad`` associated with the symbol ``squad``.  The
following example creates a ``<=`` test against the Metadata key "mad_snr" and
adds it to the list of test in ``squad``:

.. code-block:: python

  maddog = MetadataLE("mad_snr",4.0)
  squad += maddog

Finally, note it is possible to have recursive ``FiringSquad`` tests.  That is, a
``FiringSquad`` can itself contain another ``FiringSquad`` as one of the
tests set in the ``executioner_list`` passed on construction.

How to Implement an Extension
--------------------------------
Before considering developing an extension editor consider seriously if
what you need can be accomplished with one of two alternatives:

#.  Can the test be cast into a composite ``FiringSquad`` with the right components.
#.  If you need to compute some nonstandard quantity from the sample data ask
    yourself if the result can be reduced to a small set of numbers that can
    be saved as Metadata with nonstandard keys?
    If so, you can focus on the unique calculations and
    have the code post the results to Metadata.
    There is a high probability you can then
    use one or more of the classes described above to apply the needed
    test.

Anyone familiar with a basic understanding of
inheritance in an object-oriented language in general and python in particular
will recognize our implementation of all the classes described above as
textbook applications of inheritance.  A custom extension that can plug into
this same class structure must do two things:

#.   The class declaration must declare it to be a subclass of ``Executioner``.
#.   The class MUST implement a custom ``kill_if_true`` method.

In addition, almost any implementation will require a base constructor
(i.e. the line ``def __init__(self,...args..):``)
defining internal parameters that define the boundaries of the kill test.
The ``kill_if_true`` method would normally use "self" parameters set by the
constructor.

Some key points about extensions:

*  Our examples all use attributes fetched from Metadata.  That is NOT a
   requirement.  Many algorithms are possible that would compute a test
   directly from sample data.  Be warned, however, that different MsPASS data objects all have
   fundamentally different sample data organizations.  Hence, a class that
   handles sample data would require a test for the unique data to which
   it could be applied.
*  Our examples are dogmatic in requiring the data be MsPASS data objects.
   That is required because the implementation uses the kill method that
   the caller can be assured is part of the data object received.  Extensions
   that can plug in cleanly (e.g. as a member of a ``FiringSquad``) should do
   the same test for MsPASS data objects.  That test is standardized in the
   base class method ``input_is_valid``.   Use of that method in
   extensions is strongly advised to avoid unexpected aborts.
*  Consider implementing the verbose option as described here for consistency.
*  As with many things like this the best way to see how to build is an
   extension is to use the class implementations described above as examples.
