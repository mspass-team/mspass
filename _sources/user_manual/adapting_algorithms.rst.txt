.. _adapting_algorithms:

Adapting an Existing Algorithm to MsPASS
========================================

Purpose
-----------------------

The purpose of this document is to describe how an existing algorithm written
in C/C++ or python can be adapted to the MsPASS framework.  The ideas should
also readily be adapted to Fortran algorithms as well, but the authors have
no experience at this point in adapting Fortran code.  If this grows as we
hope someone in the community with experience adapting Fortran to python
may want to modify this document to describe how to do that.

The audience of this document is assumed to be core MsPASS developers who are
working directly with the MsPASS git repository.  The examples all show
how to add an algorithm to become a full component of MsPASS.   Eventually
a parallel document will be needed to produce a private, custom module that
can work as a custom extension of MsPASS.

We begin with some common requirements needed to implement a function compatible
with the framework.  The remainder of the document centers on a set of examples
of increasing complexity.

What you will need to implement
---------------------------------

Required
~~~~~~~~~~~~~~~~~~~~~~~~~

The following must be satisfied by an implementation to mesh with the MsPASS
framework.

* The algorithm must be encapsulated in a single computing function form.
  That is it must be expressible in the form x=f(y).  We mean this in the most
  general form of object oriented languages like python.  x and y can be any
  valid object.   x can and often is void provided y is mutable (i.e. the
  function acts like as FORTRAN subroutine).  y also is rarely a single symbol
  but is usually a mix of simple parameters and arbitrarily complex object.
  In most cases the arguments would be typical python key-default value pairs.

* All data passed in and out of any such function must have support for
  "pickle".   This is trivial for simple types like integers and floats, but
  is not a given for an arbitrary data structure.  Support for pickle is required
  if the algorithm is to be run under spark because in a multiprocessor
  environment the scheduler has to automatically move data between
  processing nodes.  Spark does this under the hood with pickle.

* The algorithm must not use any graphical displays or interactive user
  input of any kind.  MsPASS is designed for massive processing in a background
  mode.  Graphical displays and interactive inputs are not compatible with this
  model.

* The algorithm must never throw an unhandled exception or abort for
  any reason other than a fatal system error.  Unhandled exceptions on
  one piece of data can abort an entire job, so bombproof implementations
  are essential.   We describe some tricks below to assure this constraint is
  satisfied.

Optional but recommended
~~~~~~~~~~~~~~~~~~~~~~~~~
* MsPASS has a global level and object level history mechanism an algorithm
  should implement to be fully integrated in the framework.  The history mechanisms were
  implemented to enhance the community's ability to reproduce work done by
  others to validate previous results.  It can also provide a mechanism to
  not have to constantly "recreate the wheel" and add to an existing workflow
  instead of having to start from scratch.   Use of a nonstandard module
  that does not use the history features may break preservation of the
  workflow history chain.  In any case, as we consider in detail below
  history preservation should always be made optional.

* Many of the constraints of MsPASS are more easily satisfied if the
  algorithm uses MsPASS seismic data objects as inputs and outputs and
  converting to alternative structures required by a legacy
  algorithm with some kind of wrapper code.  e.g. Many legacy algorithms
  have interwoven dependencies on SAC or segy file images.   At some cost
  of efficiency many such algorithms can be adapted with converters that
  reorganize mspass Metadata attributes and build the fixed data structures
  such algorithms depend upon.  An example of how to do this can be see in
  the wrappers we developed for adapting obspy to the framework.

* If the algorithm you are adapting is in C or C++ you will need a means to
  create wrapper code to expose the C/C++ code to the python interpreter.
  We use pybind11 and the examples below focus on how to use that package
  to build these wrappers.  There are other packages like swig,
  boost python, and others (including low level C code wrappers)
  for accomplishing this requirement, but for ease of compatibility we
  recommend using pybind11 unless there is a compelling reason to do
  otherwise.

Example 1:  Simple function - Time Windowing Function
--------------------------------------------------------

This example illustrates how we adapted an existing C++ algorithm to the
MsPASS framework.   The objective was to produce a standard processing
function to cut smaller time windows of data from a longer (in time)
segment of data.

For us the cleanest starting point for this goal was an existing C++
function in the seispp library that Pavlis had used extensively for
some time.   The prototype for this function can be found at the
Antelope User's group contrib repository in the file
`slice_and_dice.cpp <https://github.com/antelopeusersgroup/antelope_contrib/blob/master/lib/seismic/libseispp/slice_and_dice.cc>`__

We retained the original function interface:

.. code-block:: c

  TimeSeries WindowData(const TimeSeries& parent, const TimeWindow& tw);
  ThreeComponentSeismogram WindowData(const ThreeComponentSeismogram& parent, const TimeWindow& tw);

with two changes.

1.  What we call a Seismogram in MsPASSs had the excessively verbose name ThreeComponentSeismogram in the older SEISPP library.

2.  To simplify the binding code in pybind11 (see below) we changed the name for the Seismogram version.

The MsPASS C++ version of these two functions became the following (we include the doxygen
comments for reference):

.. code-block:: c

  /*! \brief Extracts a requested time window of data from a parent TimeSeries object.
  It is common to need to extract a smaller segment of data from a larger
  time window of data.  This function accomplishes this in a nifty method that
  takes advantage of the methods contained in the BasicTimeSeries object for
  handling time.
  \return new Seismgram object derived from  parent but windowed by input
        time window range.
  \exception MsPASSError object if the requested time window is not inside data range
  \param parent is the larger TimeSeries object to be windowed
  \param tw defines the data range to be extracted from parent.
  */
  TimeSeries WindowData(const TimeSeries& parent, const TimeWindow& tw);
  /*! \brief Extracts a requested time window of data from a parent Seismogram object.
  It is common to need to extract a smaller segment of data from a larger
  time window of data.  This function accomplishes this in a nifty method that
  takes advantage of the methods contained in the BasicTimeSeries object for
  handling time.
  \return new Seismgram object derived from  parent but windowed by input
        time window range.
  \exception MsPASSError object if the requested time window is not inside data range
  \param parent is the larger Seismogram object to be windowed
  \param tw defines the data range to be extracted from parent.
  */
  Seismogram WindowData3C(const Seismogram& parent, const TimeWindow& tw);

A nontrivial detail we will not inflict on the reader is how we modified
the original code to MsPASS libraries.  In addition to name changes
there are some major differences in the API for TimeSeries and Seismogram
objects from their ancestors (TimeSeries and ThreeComponentSeismogram).
The current version of the implementations of these two algorithms can be
found `here <https://github.com/wangyinz/mspass/blob/master/cxx/src/lib/algorithms/slice_and_dice.cc>`__.

MsPASS uses the `pybind11 package<https://pybind11.readthedocs.io/en/stable/>`
to bind C++ or C code for use by the python interpreter.  For the present
all C/C++ code is bound to a single module we call mspasspy.ccore.
The details of the build system used in MsPASS are best discussed in a
separate document (Need a link here eventually).  This particular example
required adding the above function prototype definitions to
`this include file <https://github.com/wangyinz/mspass/blob/master/cxx/include/mspass/algorithms/algorithms.h>`__
and the C++ function code `here <https://github.com/wangyinz/mspass/blob/master/cxx/src/lib/algorithms/slice_and_dice.cc>`__.

Creating the python bindings for these two functions required inserting the
following blocks in the binding code for the algorithms module found
`here <https://github.com/wangyinz/mspass/blob/master/cxx/python/algorithms/basic_py.cc>`__:

.. code-block:: c

  m.def("_WindowData",&mspass::WindowData,"Reduce data to window inside original",
    py::return_value_policy::copy,
    py::arg("d"),
    py::arg("twin") )
  ;
  m.def("_WindowData3C",&mspass::WindowData3C,"Reduce data to window inside original",
    py::return_value_policy::copy,
    py::arg("d"),
    py::arg("twin") )
  ;

We note a few details about this block of code:

1. The :code:`m` symbol is defined earlier in this file as a tag for the module to which we aim to bind this function.
   It is defined earlier in the file with this construct:
   
   .. code-block:: c

    PYBIND11_MODULE(ccore,m)
   
   That is, this construct defines the symbol :code:`m` as an abstraction for the
   python module ccore.

2. The actual C++ function names are "WindowData" and "WindowData3C", but
   we change the names here to "_WindowData" and "_WindowData3C" respectively.
   We recommend that convention as it is conventional in python, although not really
   rigidly enforced by the language, to assume a symbol with one or
   leading underscores is "for internal use"
   (see e.g. obspy documentation or
   `this nice overview <https://www.datacamp.com/community/tutorials/role-underscore-python>`__).
   That usage is appropriate here as our next step will be to write a master python
   wrapper used as a front end to simplify the user api to this pair of
   functions that implement the same conceptual algorithm on two different types of
   data.

3. Our binding code does nothing fancy with the arguments.  The pybind11 documentation
   describes how to set default argument values.  We intentionally do not use such
   a construct here as these ccore functions should only be used through
   the master python wrapper we will discuss next (This is also why we intentionally
   wrapped the functions with a name containing a leading underscore.)


Example 2:  Pure Python Function
-----------------------------------

Example 3:  More complicated mixed C++ and python example - scale function
---------------------------------------------------------------------------
