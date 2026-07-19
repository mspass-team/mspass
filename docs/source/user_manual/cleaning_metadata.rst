.. _cleaning_metadata:

================================================
Cleaning Inconsistent Metadata
================================================
*Gary L. Pavlis*
--------------------

Concepts
----------
One of the strengths of MsPASS as a framework for research computing is the
`Metadata` container that is conceptually nearly identical to a Python dictionary.
That is, a `Metadata` container stores attributes accessible by a string-valued
key using constructs like the following:  :code:`x=d['sta']`.  It provides
a way to more clearly handle things that define what the
modern concept of "Metadata" means.   The dark side of the flexibility
of containers like `Metadata` or a Python dictionary is that the information
they contain can easily become stale and/or inconsistent with the data
with which they are associated.  A common case in MsPASS is when
a set of three `TimeSeries` objects are run through the
:py:func:`mspasspy.algorithms.bundle.bundle_seed_data` function to create a
`Seismogram` object.  The current implementation removes ``chan`` and ``loc``
from live output, but other attributes cloned from one input component, such as
``channel_id`` and ``channel_lat``, can remain.  Such attributes are
inconsistent with the concept of a `Seismogram`, which is defined as an
assembled bundle of three channels, because a single value refers to only one
of the three components.

The example of the issue created by the
:py:func:`mspasspy.algorithms.bundle.bundle_seed_data` function
could be solved by additional modifications to that function.
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

:py:class:`mspasspy.util.Janitor.Janitor` has several processing methods
that can be used to handle junk differently:

#. :py:meth:`mspasspy.util.Janitor.Janitor.clean` silently discards
   all attributes not in the list of "keepers".  It edits and returns the
   input datum.
#. :py:meth:`mspasspy.util.Janitor.Janitor.bag_trash` does not
   discard attributes it treats as trash but bundles them up into
   a Python dictionary (the bag), removes the originals, and posts the trash
   bag content with a user-defined key.  It edits and returns the input with
   the trash attributes placed in that dictionary.
#. :py:meth:`mspasspy.util.Janitor.Janitor.collect_trash` is best
   thought of as a lower-level function most users are unlikely to need.
   Unlike the other methods of :code:`Janitor`, it returns the trash as a
   Python dictionary after removing those attributes from the datum.
   It is called by the :py:meth:`mspasspy.util.Janitor.Janitor.bag_trash`
   to create the Python dictionary it posts back to the datum it is
   handling.  This method exists largely to allow alternative ways to
   handle the trash.
#. :py:meth:`mspasspy.util.Janitor.Janitor.add2keepers` adds a key to an
   existing cleaner at run time.  This is useful when a processing stage adds
   a legitimate attribute that is not in the YAML definition.

For an ensemble, all three processing methods examine the ensemble's own
`Metadata` container.  :code:`clean` and :code:`bag_trash` also process every
member when :code:`process_ensemble_members=True`, the default.
:code:`collect_trash` processes only the container passed to it.  The stock
YAML files do not define an ``Ensemble`` keeper list, so that list is empty by
default.  Add keys with ``add2keepers`` before cleaning an ensemble if its
container Metadata must be retained.  Janitor processing methods leave dead
input unchanged.

Note that in all cases an instance of a :code:`Janitor` needs to be
instantiated before it can be used.   The constructor initializes
the list of "keepers" for different seismic data types.  The default
constructor reads a YAML-format file distributed with MsPASS (from
``$MSPASS_HOME/data/yaml`` when that environment variable is set, otherwise
from the installed package data).  The default list is easily changed by
creating a new YAML-format file and passing its path to the
class constructor via the :code:`keepers_file` argument.   See the docstring for
:py:class:`mspasspy.util.Janitor.Janitor` for details.

Do not confuse :code:`Janitor` with
:py:class:`mspasspy.util.Undertaker.Undertaker`.  Janitor cleans the Metadata
namespace of in-memory, live data.  Undertaker handles dead or aborted data,
normally during database writes.  For documents already stored in MongoDB,
:py:meth:`mspasspy.db.database.Database.clean_collection` provides a separate,
schema-driven database cleanup operation.

Examples
-----------
Usage
^^^^^^^
The examples below illustrate a range of applications of the
:code:`Janitor` class.  The later workflow examples are sketches with
application-specific initialization omitted.  They are intended as
starting points to aid development of workflows using
this class.

Basic usage
^^^^^^^^^^^^
This trivial example constructs a live :code:`TimeSeries` with minimal
Metadata.  The script adds an undefined Metadata value of the "foo-bar"
construct used in many tutorials.   The assert statements
verify that the :code:`clean` method clears the debris:

.. code-block:: python

   from mspasspy.util.Janitor import Janitor
   from mspasspy.ccore.seismic import TimeSeries

   # assign a default-constructed Janitor to cleaner
   cleaner = Janitor()
   datum = TimeSeries(10)
   datum.set_live()
   # assign a metadata key-value pair not in keepers list of cleaner
   datum["foo"] = "bar"
   assert "foo" in datum
   datum = cleaner.clean(datum)
   assert "foo" not in datum

Application to ensembles
^^^^^^^^^^^^^^^^^^^^^^^^^^
The second example below demonstrates an ambiguity the :code:`Janitor`
has to handle.  With an ensemble object there are two things
the cleaner may have to take care of:  (1) the :code:`Metadata` for
the ensemble object itself, and (2) the content of the atomic data
that are bundled in the ensemble.  The keeper lists and the
``process_ensemble_members`` constructor argument control that behavior:

.. code-block:: python

   from mspasspy.util.Janitor import Janitor
   # this helper is available when running in the source test environment
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
   # Default behavior cleans both the ensemble container and its members.
   # Preserve badkey explicitly so this pass demonstrates member cleaning.
   member_cleaner = Janitor()
   member_cleaner.add2keepers("badkey", "ensemble")
   e1 = member_cleaner.clean(e1)
   # removes foo from members
   for d in e1.member:
     assert "foo" not in d
   # badkey was explicitly retained in the ensemble Metadata container
   assert "badkey" in e1
   assert e1["badkey"] == "badvalue"
   # Now clean only the ensemble container.
   ensemble_cleaner = Janitor(process_ensemble_members=False)
   e1 = TimeSeriesEnsemble(e_save)
   # note the asserts below all have the reverse logic of above
   e1 = ensemble_cleaner.clean(e1)
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
which in this case is always ``"mseed"``,
are an example of an attribute that is inconsistent
with the data once a :code:`TimeSeries` object is constructed from
a miniseed file or file image.  Because miniseed data are the most common
starting point for most seismology workflows, there is a special
subclass of :py:class:`mspasspy.util.Janitor.Janitor` called
:py:class:`mspasspy.util.Janitor.MiniseedJanitor`.   It differs
only in initialization: its default YAML file is specialized
for reading from raw miniseed data.   This class should only be used
immediately after reading from *wf_miniseed* records.  The following
is a sketch of a typical algorithm:

.. code-block:: python

    from mspasspy.util.Janitor import MiniseedJanitor

    janitor = MiniseedJanitor()
    # assumes symbol db is a database handle constructed earlier
    cursor = db.wf_miniseed.find({})
    for doc in cursor:
        d = db.read_data(doc, collection="wf_miniseed")
        d = janitor.clean(d)
        # additional processing functions follow

Parallel workflow
^^^^^^^^^^^^^^^^^^^
This is a sketch of a code segment illustrating the use of a
:code:`Janitor` in a parallel workflow.  The example reads
a collection of :code:`TimeSeriesEnsemble` objects, runs the
:py:func:`mspasspy.algorithms.bundle.bundle_seed_data` function to
convert each to a :code:`SeismogramEnsemble`, and then
runs the instance of :code:`Janitor` before saving the results.

.. code-block:: python

   from mspasspy.algorithms.basic import rotate_to_standard
   from mspasspy.algorithms.bundle import bundle_seed_data
   from mspasspy.io.distributed import (
       read_distributed_data,
       write_distributed_data,
   )
   from mspasspy.util.Janitor import Janitor

   # Database initialization would go above this point.
   janitor = Janitor()
   # generate a list of queries defining all common source gathers
   # defined in the data set
   srcids=db.wf_TimeSeries.distinct("source_id")
   queries=list()
   for sid in srcids:
      queries.append({"source_id" : sid})
   # parallel job using parallel reader and writer
   mydata = read_distributed_data(
       queries, db, collection="wf_TimeSeries"
   )
   mydata = mydata.map(bundle_seed_data)
   mydata = mydata.map(rotate_to_standard)
   mydata = mydata.map(janitor.clean)
   saved_ids = write_distributed_data(mydata,
                                      db,
                                      collection="wf_Seismogram",
                                      data_are_atomic=False,
                                      )

The writer is the terminal operation and initiates the Dask or Spark
computation.  Janitor leaves dead input unchanged; the writer uses
:py:class:`mspasspy.util.Undertaker.Undertaker` to bury dead results by
default.  Set the writer's ``cremate=True`` only when ordinary dead-data
remains need not be preserved.
