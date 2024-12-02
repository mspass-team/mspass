.. _cleaning_metadata:

================================================
Cleaning Inconsistent Metadata
================================================
*Gary L. Pavlis*
--------------------

Concepts
----------
One of the strengths of MsPASS as a framework for research computing is the
`Metadata` container that is conceptually nearly identical to a python dictionary.
That is, a `Metadata` container stores attributes accessible via a string-valued
using constructs like the following:  :code:`x=d['sta']`.
as a way to more clearly handle things that define what the
modern concept of "Metadata" means.   The dark side of the flexibility
of containers like `Metadata` or a python dictionary is that the information
they contain can easily become stale and/or inconsistent with the data
with which they are associated.  A common case in MsPASS is when
a set of three `TimeSeries` objects are run through the
:py:func:`mspasspy.algorithms.bundle.bundle` function to create a `Seismogram`
object.   That function leaves debris from cloning the parents
related to the single channel inputs.   That is, attributes with
keys "chan", "loc", "channel_id", "channel_lat", etc.  Those attributes
are inconsistent with the concept of a `Seismogram`, which is defined
as an assembled bundle of three channels.   The keys are inconsistent
because the data are defined by three different components and the ones
defined are always referring to only one of the three components.

The example of the issue created by the
:py:func:`mspasspy.algorithms.bundle.bundle` function
could been solved by appropriate modifications to that function.
The problem of functions creating stale/inconsistent metadata,
however, is ubiquitous.  For that reason we developed
a generic solution in MsPASS.   We implemented a metadata cleaner
class with the mnemonic name :py:class:`mspasspy.util.Janitor.Janitor`.
The class has methods to clear inconsistent Metadata from data
during processing to reduce the volume of junk attributes stored in
the database.   The remainder of this section uses examples to illustrate
the use of an instance of a :py:class:`mspasspy.util.Janitor.Janitor`.

Janitor class
--------------
An instance of a :code:`Janitor` can be thought of as a robotic cleaner that
keeps only attributes it is told to keep.  An analogy for a physical janitor
is a person with instructions to clean out an office and retain only papers,
pens, or pencils on a desk.  Throw everything else away.  For
seismic processing an instance of a :code:`Janitor` is told what keys to
retain in any Metadata container.  Any key not defined in that list is to
be treated as junk/trash.

:py:class:`mspasspy.util.Janitor.Janitor` has three processing methods
that can be used to handle junk differently:

#. :py:meth:`mspasspy.util.Janitor.Janitor.clean` silently discards
   all attributes not in the list of "keepers".  It returns an edited
   copy of the input.
#. :py:meth:`mspasspy.util.Janitor.Janitor.bag_trash` does not
   discard attributes it treats as trash but bundles them up into
   a python dictionary (the bag), removes the originals, and posts the trash
   bag content with a user defined key. It returns and edited copy of
   the input with the trash attributes removed and placed in the bag
   python dictionary.
#. :py:meth:`mspasspy.util.Janitor.Janitor.collect_trash` is best
   thought of as a lower-level function most users are unlikely to need.
   Unlike the other methods of :code:`Janitor`, it returns a
   python dictionary that the trash bag.
   It is called by the :py:meth:`mspasspy.util.Janitor.Janitor.bag_trash`
   to create the python dictionary it posts back to the datum it is
   handling.  This method exists largely to allow alternative ways to
   handle the trash.

Note that in all cases an instance of a :code:`Janitor` needs to be
instantiated before it can be used.   The constructor initializes
the list of "keepers" for different seismic data types.  The default
constructor reads from yaml format file found in the mspass source
code tree in data/yaml.  The default list is easily changed by
created a new yaml format file and passing that file name to the
class constructor via the :code:`keepers_file` argument.   See the docstring for
:py:class:`mspasspy.util.Janitor.Janitor` for details.

Examples
-----------
Usage
^^^^^^^
The examples below illustrate a range of applications of the
:code:`Janitor` class.   None of them are complete python scripts
that can be run as all have incomplete initializations.   The examples
are intended to starting points to aid development of workflows using
this class.

Basic usage
^^^^^^^^^^^^
This is a trivial example that is a variant of
part of the pytest module used for this
class.  It uses a test function to generate an :code:`TimeSeries`
object with zeros in the data array and minimal Metadata.
The script adds an undefined Metadata value of the "foo-bar"
construct used in many tutorials.   The assert statements
verify that the :code:`clean` method clears the debris:

.. code-block:: python

   from mspasspy.util.Janitor import Janitor
   # this will resolve only when run with pytest
   from helper import get_live_timeseries
   # default constructor assign to the symbol cleaner
   cleaner = Janitor()
   datum = get_live_timeseries()
   # assign a metadata key-value pair not in keepers list of cleaner
   datum["foo"] = "bar"
   assert "foo" in datum
   datum = cleaner.clean(datum)
   assert "foo" not in datum

Application to ensembles
^^^^^^^^^^^^^^^^^^^^^^^^^^
The second example below demonstrates an ambiguity the :code:`Janitor`
has to handle.   That is, with ensemble object there are two things
the cleaner may have to take care of:  (1) the :code:`Metadata` for
the ensemble object itself, and (2) the content of the atomic data
that are bundled in the "ensemble".   That is handled by the constructor
and the difference in the usage is displayed in this example:

.. code-block:: python

   from mspasspy.util.Janitor import Janitor
   # this will resolve only if run with pytest
   from helper import get_live_timeseries_ensemble
   from mspasspy.ccore.seismic import TimeSeriesEnsemble
   # generate a junk ensemble with 3 members using helper function
   e1 = get_live_timeseries_ensemble(3)
   # add undefined key-value pair to each ensemble member
   for i in range(len(e1.member)):
     e1.member[i]["foo"] = "bar"
   # add an invalid key to the ensemble's metadata
   e1["badkey"] = "badvalue"
   # use the copy constructor for this object from C++ bindings as best practice
   e_save = TimeSeriesEnsemble(e1)
   # default Janitor behavior cleans members
   member_cleaner = Janitor()
   e1 = member_cleaner.clean(e1)
   # removes foo from members
   for d in e1.member:
     assert "foo" not in d
   # does not alter ensemble Metadata container
   assert "badkey" in e1
   assert e1["badkey"] == "badvalue"
   # Now create an instance that does the opposite.
   ensemble_cleaner = Janitor(process_ensemble_members=False)
   e1 = TimeSeriesEnsemble(e_copy)
   # note the asserts below all have the reverse logic of above
   e1 = member_cleaner.clean(e1)
   # now foo is still in all members
   for d in e1.member:
     assert "foo" in d
   # Now the ensemble has badkey cleared
   assert "badkey" not in e1

Miniseed Data
^^^^^^^^^^^^^^^
The MsPASS indexing function for miniseed data loads the common content of
miniseed packet headers and several computed quantities like
start time and end time.   Some of those like the "format" attribute,
which in this case is always "miniseed",
are an example of an attribute that is inconsistent
with the data once a :code:`TimeSeries` object is constructed from
a miniseed file or file image.  Because miniseed data are the most common
starting point for most seismology workflows, there is a special
subclass of :py:class:`mspasspy.util.Janitor.Janitor` called
:py:class:`mspasspy.util.Janitor.MiniseedJanitor`.   It differs
only in the initialization where the default yaml file is specialized
for reading from raw miniseed data.   This class should only be used
immediately after reading from *wf_miniseed* records.  The following
is a sketch of a typical algorithm:

.. code-block:: Python

    from mspasspy.util.Janitor import MiniseedJanitor
      ... additional initializations ...

    janitor = MiniseedJanitor()
    # assumes symbol db is a database handle constructed earlier
    cursor = db.wf_miniseed.find({})
    for doc in cursor:
        d = db.read_data(doc,collection="wf_miniseed")
        d = janitor.clean(d)
          ... additional processing functions ...

Parallel workflow
^^^^^^^^^^^^^^^^^^^
This is a sketch of a code segment illustrating the use of a
:code:`Janitor` in a parallel workflow.  The example reads
a collection of :code:`TimeSeriesEnsembles`, runs the
:py:func:`mspasspy.algorithms.bundle.bundle` function to
convert each to a :code:`SeismogramEnsemble`, and then
runs the instance of :code:`Janitor` before saving the results.

.. code-block:: Python

     ... Initialization code would go above this point ...
   janitor = Janitor()
   # generate a list of queries defining all common source gathers
   # defined in the data set
   srcids=db.wf_TimeSeries.distinct("source_id")
   queries=list()
   for sid in srcids:
      queries.append({"source_id" : sid})
   # parallel job using parallel reader and writer
   mydata = read_distributed_data(queries,collection="wf_TimeSeries")
   mydata = mydata.map(rotate_to_standard)
   mydata = mydata.map(bundle)
   mydata = mydata.map(janitr.clean)
   saved_ids = write_distributed_data(mydata,
                                  db,
                                  collection="wf_Seismogram",
                                  data_are_atomic=False,
                                )
