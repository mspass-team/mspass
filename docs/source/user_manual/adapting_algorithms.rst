.. _adapting_algorithms:

Adapting an Existing Algorithm to MsPASS
========================================

Purpose
-----------------------

The purpose of this document is to describe how an existing algorithm written
in C/C++ or Python can be adapted to the MsPASS framework.  The ideas should
also readily be adapted to Fortran algorithms as well, but the authors have
no experience at this point in adapting Fortran code.  If this grows as we
hope someone in the community with experience adapting Fortran to python
may want to modify this document to describe how to do that.

The audience of this document is assumed to be core MsPASS developers who are
working directly with the MsPASS Git repository.  The examples all show
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

* All data passed in and out of any such function must be serializable by the
  selected distributed framework.  This is trivial for simple types like
  integers and floats, but is not a given for an arbitrary data structure.
  Both Dask and Spark may move functions, arguments, and results between
  worker processes, using pickle-compatible serialization where appropriate.

* The algorithm must not use any graphical displays or interactive user
  input of any kind.  MsPASS is designed for massive processing in a background
  mode.  Graphical displays and interactive inputs are not compatible with this
  model.

* Expected data errors must not escape and abort a distributed job.  MsPASS
  algorithms normally log the error in the datum's ``elog``, mark unusable
  data dead, and return it.  The standard decorators catch ``RuntimeError``,
  non-fatal ``MsPASSError``, and other ordinary exceptions.  Fatal
  ``MsPASSError`` and system-level interrupts still propagate by design.

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
Antelope Users Group contrib repository in
`slice_and_dice.cc
<https://github.com/antelopeusersgroup/antelope_contrib/blob/master/lib/seismic/libseispp/slice_and_dice.cc>`__.

We retained the original function interface:

.. code-block:: c

  TimeSeries WindowData(const TimeSeries& parent, const TimeWindow& tw);
  ThreeComponentSeismogram WindowData(const ThreeComponentSeismogram& parent, const TimeWindow& tw);

The current MsPASS interface makes two relevant changes.

1.  What MsPASS calls a ``Seismogram`` had the longer name
    ``ThreeComponentSeismogram`` in SEISPP.

2.  C++ overloads both variants under the name ``WindowData``.  The pybind11
    layer exposes distinct internal Python names because Python does not
    dispatch overloads by static argument type.

The public-object overloads are currently declared as follows:

.. code-block:: cpp

  mspass::seismic::TimeSeries
  WindowData(const mspass::seismic::TimeSeries& parent,
             const mspass::algorithms::TimeWindow& tw);

  mspass::seismic::Seismogram
  WindowData(const mspass::seismic::Seismogram& parent,
             const mspass::algorithms::TimeWindow& tw);

There are also overloads for ``CoreTimeSeries`` and ``CoreSeismogram``.  The
public ``TimeSeries`` and ``Seismogram`` variants handle an invalid window by
returning dead data with an ``elog`` entry; the Core variants can throw
``MsPASSError``.

A nontrivial detail we will not inflict on the reader is how we modified
the original code to MsPASS libraries.  In addition to name changes
there are some major differences in the API for TimeSeries and Seismogram
objects from their ancestors (TimeSeries and ThreeComponentSeismogram).
The current implementations are in
`slice_and_dice.cc
<https://github.com/mspass-team/mspass/blob/master/cxx/src/lib/algorithms/slice_and_dice.cc>`__.

MsPASS uses the `pybind11 package <https://pybind11.readthedocs.io/en/stable/>`
to bind C++ or C code for use by the Python interpreter.  ``mspasspy.ccore``
is a package containing several compiled extension modules; this example is
bound in ``mspasspy.ccore.algorithms.basic``.  See
:ref:`advanced_setup_considerations` for the source-build workflow.  A new
C++ algorithm must also be added to the appropriate CMake source/extension
target.  This example declares its prototypes in
`algorithms.h
<https://github.com/mspass-team/mspass/blob/master/cxx/include/mspass/algorithms/algorithms.h>`__
and implements them in the ``slice_and_dice.cc`` file linked above.

Creating the python bindings for these two functions required inserting the
following blocks in
`basic_py.cc
<https://github.com/mspass-team/mspass/blob/master/cxx/python/algorithms/basic_py.cc>`__:

.. code-block:: cpp

  m.def("_WindowData",
        py::overload_cast<const TimeSeries&, const TimeWindow&>(&WindowData),
        "Reduce data to window inside original",
        py::return_value_policy::copy,
        py::arg("d"),
        py::arg("twin"));

  m.def("_WindowData3C",
        py::overload_cast<const Seismogram&, const TimeWindow&>(&WindowData),
        "Reduce data to window inside original",
        py::return_value_policy::copy,
        py::arg("d"),
        py::arg("twin"));

We note a few details about this block of code:

1. The :code:`m` symbol is the pybind11 module object.  This file defines it
   with:

   .. code-block:: cpp

      PYBIND11_MODULE(basic, m)

   The module then sets its full Python name to
   ``mspasspy.ccore.algorithms.basic``.

2. Both C++ overloads are named ``WindowData``.  ``py::overload_cast`` selects
   the desired signature and the binding exposes ``_WindowData`` and
   ``_WindowData3C``.  A leading underscore conventionally marks a Python
   symbol as non-public; see the
   `Python tutorial on private names
   <https://docs.python.org/3/tutorial/classes.html#private-variables>`__.
   That usage is appropriate here as our next step will be to write a master python
   wrapper used as a front end to simplify the user api to this pair of
   functions that implement the same conceptual algorithm on two different types of
   data.

3. Our binding code does nothing fancy with the arguments.  The pybind11 documentation
   describes how to set default argument values.  We intentionally do not use such
   a construct here as these ccore functions should only be used through
   the master python wrapper we will discuss next (This is also why we intentionally
   wrapped the functions with a name containing a leading underscore.)


Example 2: Python front end and decorators
------------------------------------------

The public implementation is
:func:`mspasspy.algorithms.window.WindowData`.  Its internal
``WindowDataAtomic`` helper dispatches ``TimeSeries`` to ``_WindowData`` and
``Seismogram`` to ``_WindowData3C``.  The public function adds ensemble
handling, short-segment policies, error logging, and object-history support.
It is decorated with
:func:`~mspasspy.util.decorators.mspass_func_wrapper`.

A new atomic function commonly starts with this pattern:

.. code-block:: python

   from mspasspy.util.decorators import mspass_func_wrapper

   @mspass_func_wrapper
   def my_algorithm(
       data,
       parameter,
       *args,
       object_history=False,
       alg_name="my_algorithm",
       alg_id=None,
       dryrun=False,
       handles_ensembles=False,
       checks_arg0_type=False,
       handles_dead_data=False,
       **kwargs,
   ):
       return _bound_algorithm(data, parameter)

With these settings, the decorator validates arg0, returns dead input without
calling the function, and applies the atomic function to each ensemble member.
Set ``handles_ensembles=True`` only when the function itself implements
ensemble behavior.  Use ``inplace_return=True`` for a function that mutates
arg0 and returns ``None``.  Object history is off by default; when callers set
``object_history=True``, they must also supply ``alg_id``.  For a class method
whose first data argument follows ``self``, use
:func:`~mspasspy.util.decorators.mspass_method_wrapper`.  Functions consuming
two MsPASS data objects require
:func:`~mspasspy.util.decorators.mspass_func_wrapper_multi` or explicit error
and history handling.

Example 3: Mixed C++ and Python scale function
----------------------------------------------

:func:`mspasspy.algorithms.window.scale` is a more complicated current
example.  It dispatches atomic and ensemble inputs to bindings in
``mspasspy.ccore.algorithms.amplitudes``, validates method-specific arguments,
updates ``calib`` metadata, and uses ``mspass_func_wrapper`` for common
history, dry-run, and error behavior.  Study the
`Python front end
<https://github.com/mspass-team/mspass/blob/master/python/mspasspy/algorithms/window.py>`__
together with the
`amplitude bindings
<https://github.com/mspass-team/mspass/blob/master/cxx/python/algorithms/amplitudes_py.cc>`__;
neither layer alone defines the complete public contract.

Validation
----------

After changing a binding, rebuild or reinstall MsPASS in the active source
environment, verify that the compiled symbol imports from its full
``mspasspy.ccore`` submodule, and add focused C++ and Python tests.  For this
example, ``python/tests/algorithms/test_window.py`` exercises the public
wrapper, while the C++ implementation and binding source should be covered by
the relevant native tests and an import check.
