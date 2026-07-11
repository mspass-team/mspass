.. _numpy_scipy_interface:

Using NumPy and SciPy with MsPASS
============================================
Overview
-------------
The `NumPy <https://numpy.org/>`__ and `SciPy <https://scipy.org/>`__ packages are heavily used for scientific computing
in a wide range of fields.   One of the reasons for that is performance.
Built-in Python containers are not designed for high-performance numerical
linear algebra.
NumPy provides typed multidimensional arrays and compiled numerical kernels;
SciPy builds additional scientific algorithms on that foundation.  ObsPy also
uses NumPy arrays for waveform samples.  As a result, an early
design constraint in MsPASS was to provide as clean an interface to
NumPy and SciPy as possible.  However, because we chose to implement our own
core data objects in C++, that presents a disconnect that causes a few
anomalies in the MsPASS API.  This section of the User's Manual
documents these issues and provides guidance for mixing NumPy and SciPy
algorithms with :py:class:`TimeSeries<mspasspy.ccore.seismic.TimeSeries>`,
:py:class:`Seismogram<mspasspy.ccore.seismic.Seismogram>`, and ensemble
objects.  The experimental regular-grid
:py:class:`Gather<mspasspy.seismic.gather.Gather>` and
:py:class:`SeismogramGather<mspasspy.seismic.gather.SeismogramGather>` types
use array-oriented storage directly and are outside this page's scope.

TimeSeries Data
-----------------
Recall that the MsPASS
:py:class:`TimeSeries<mspasspy.ccore.seismic.TimeSeries>` class represents
a single channel of fixed sample rate seismic data spanning a given time period.
As such we used the standard signal processing abstraction of storing
the data as a vector of numbers.

Every computing language that has any role in numerical computing defines
a syntax for arrays.  A vector in this context is a one-dimensional array.
A Python list is not the same thing as an array in Fortran or C.  Both Fortran and C define
an array as a fixed block of memory of a specified length containing
a sequence of numbers of the same type.   (e.g. a C double array of length
10 is a block of :math:`8 \times 10` bytes.)  NumPy performance is achieved by
redefining an array to assume the data are in a contiguous memory block and
using wrappers that allow low-level operations to actually be performed
by compiled C or FORTRAN functions.  We do the same thing in MsPASS
but our data objects do not contain NumPy array classes directly.  Instead,
we use the same approach to allow low-level operations to be
done in C++.  Because we use the same concept (arrays are contiguous
blocks of memory) the arrays can mostly be used interchangeably, but
not always.

So what is the difference?   If you execute this code fragment in MsPASS:

.. code-block:: python

  import numpy as np
  from mspasspy.ccore.seismic import DoubleVector, TimeSeries
  x_numpy = np.zeros(100)
  x_mspass = TimeSeries(100)
  print("Type of numpy object=",type(x_numpy))
  print("Type of mspass data vector=",type(x_mspass.data))

You should get this output::

  Type of numpy object= <class 'numpy.ndarray'>
  Type of mspass data vector= <class 'mspasspy.ccore.seismic.DoubleVector'>

The point is that both define two conceptually similar things that are a
vector of sample data.  In this example
these are stored with the symbols ``x_numpy`` and ``x_mspass.data``.
The print statements emphasize they are
different data types.  They are not, however, an apples-to-oranges
kind of comparison but more of a Jonathan versus Gala apple comparison.
Python APIs often use "duck typing": an operation accepts objects that
provide the required behavior rather than demanding one concrete class.
Because of "duck typing" we can, in most cases, interchange the use of the
two classes `ndarray` and `DoubleVector`.
For example, you can usually send ``DoubleVector`` data to a
NumPy or SciPy function that accepts array-like input.  Here are a few
examples:

.. code-block:: python

  import numpy as np
  from scipy.signal import hilbert
  # Setting data to nonzero values - not essential but otherwise
  # trivial zero vector will cause problems for the log example
  for i in range(x_mspass.npts):
    x_mspass.data[i]=float(i+1)
  x = np.log10(x_mspass.data)
  y = np.sqrt(x_mspass.data)
  z=np.cos(x_mspass.data)
  yh = hilbert(x_mspass.data)

That works because ``DoubleVector`` is what NumPy's documentation
calls array-like.  In fact, any NumPy or SciPy
function that accepts an array-like argument can generally accept the ``data``
member of a ``TimeSeries``.  In reality, ``DoubleVector`` is
a C++ "vector container" that in C++ would be declared as:

.. code-block:: C

  std::vector<double> x;

In MsPASS we use stock "bindings" from pybind11 that allow us to do
this behavior.  The buffer protocol also lets
``np.asarray(x_mspass.data)`` create a writable NumPy view without copying.
Mutating that view therefore mutates the ``TimeSeries`` samples; call
``np.array(x_mspass.data, copy=True)`` when isolation is required.

This works for most, but not all, vector operators that
mix ``DoubleVector`` and ``ndarray`` types.  For example, with the symbols above defined
all of the following will work and do what is expected:

.. code-block:: python

  z=x_numpy-x_mspass.data
  z=x_mspass.data+x_numpy
  x_mspass.data -= z
  z=x_numpy*x_mspass.data  # a peculiar numpy operation - not a dot product
  x_mspass.data *= 10.0

The main thing to avoid is an assignment where the left hand side
and right hand side resolve to different types.   For instance,
the following will fail with a ``TypeError`` exception:

.. code-block:: python

  z=np.ones(100)
  x_mspass.data = x_numpy
  x_mspass.data = x_numpy + z

The solution is to construct a ``DoubleVector`` explicitly:

.. code-block:: python

  x_mspass.data = DoubleVector(x_numpy)
  x_mspass.data = DoubleVector(x_numpy + z)

That is necessary because the right-hand side is an ``ndarray``.  The
``DoubleVector`` constructor copies and converts the values.

Be aware that if you mix types in a vector operation you can get some surprising
results.  For example, the following code generates a ``TypeError``
exception:

.. code-block:: python

  ts=TimeSeries(100)
  z=np.ones(100)
  ts.data = z + ts.data

while reversing the order of ``z`` and ``ts.data``

.. code-block:: python

  ts=TimeSeries(100)
  z=np.ones(100)
  ts.data = ts.data + z

works and sets ``ts.data`` to the vector sum of the two symbols
on the right-hand side.  The result type follows the implementation selected
by Python's binary-operator dispatch, so operand order matters.  Avoid relying
on that detail; make the conversion explicit:

.. code-block:: python

  ts=TimeSeries(100)
  z = np.ones(100)
  x = z + np.asarray(ts.data)
  ts.data = DoubleVector(x)

Current ``DoubleVector`` bindings support ordinary Python slices.  A slice
returns a new ``DoubleVector``:

.. code-block:: python

  ts = TimeSeries(100)
  x = ts.data[5:10]
  assert isinstance(x, DoubleVector)
  assert len(x) == 5

NumPy is still more convenient for advanced indexing.  The important
invariant is that ``ts.npts`` must equal ``len(ts.data)``.  Assigning a
shorter vector does not update ``npts`` automatically, so update both:

.. code-block:: python

  ts = TimeSeries(100)
  x = np.ones(100)[5:10]
  ts.set_npts(len(x))
  ts.data = DoubleVector(x)
  assert ts.npts == len(ts.data) == 5

Seismogram Data
~~~~~~~~~~~~~~~~~
MsPASS uses a different data type to hold data from three-component sensors.
The motivation is described in the section titled
:ref:`Data Object Design Concepts<data_object_design_concepts>`.
The result is that the array holding the sample data is a matrix
instead of a vector.  Some implementation details of note are:

1.  In MsPASS the matrix has 3 rows and ``npts`` columns.  That means the
    rows of the matrix can be extracted to create a ``TimeSeries``
    subset of the data while the columns are single "vector" samples
    of ground motion.
2.  Currently MsPASS uses a simple, lightweight matrix class to store the sample
    data called :py:class:`dmatrix<mspasspy.ccore.utility.dmatrix>`.  A key reason we
    made that choice was control over methods defined for the class.  A
    case in point is the ``shape`` attribute, which is important when working
    with NumPy.
3.  For performance ``dmatrix`` is implemented in C++.  The class has methods
    for standard matrix operations with Python bindings for those operators.
    When working with ``Seismogram`` data,
    use the native operators when possible.
4.  A ``dmatrix`` stores the array data in a contiguous block of memory
    in what NumPy calls Fortran order.  Fortran stores a matrix with the
    row index "varying fastest".  Exchanging the sample data stored
    this way with NumPy is like that with the ``std::vector`` used in
    ``TimeSeries``.  ``np.asarray(seismogram.data)`` provides a writable view
    of that buffer.
5.  We use a variation of the same "magic" in the pybind11 binding code
    that allows the ``Seismogram.data`` matrix to interact cleanly with
    NumPy and SciPy functions that require a matrix.  Like the scalar
    case there are impedance mismatches, however, that can complicate
    that exchange. We discuss these below.

Almost all of what was discussed above about using `TimeSeries`
arrays with NumPy are similar.  For example, although the result is
meaningless, you can run the following code snippet with
MsPASS and it will run and produce the expected result:

.. code-block:: python

  import numpy as np
  from mspasspy.ccore.seismic import Seismogram
  from mspasspy.ccore.utility import dmatrix
  x_mspass = Seismogram(100)
  # initialize the matrix to some meaningless, nonzero values
  for i in range(3):
    for j in range(x_mspass.npts):
       x_mspass.data[i,j]=i+j+1
  # these apply the math operation for each number in the data matrix
  z=np.cos(x_mspass.data)
  z=np.exp(x_mspass.data)
  z=np.sqrt(x_mspass.data)
  # matrix operation that multiplies all samples by 2.0
  x_mspass.data *= 2.0
  # matrix operator adding content of numpy matrix z to data matrix
  y=x_mspass.data + z
  # matrix operation adding y and x_mspass.data matrices
  x_mspass.data += y
  # Same thing done with a binary + operator
  # use a copy to show more likely use
  z=Seismogram(x_mspass)
  x_mspass.data = z.data + y

As with ``TimeSeries``, a mismatch occurs if the operation
yields a type mismatch.  For example, the following minor variant of
above will run:

.. code-block:: python

  z=Seismogram(x_mspass)
  y=np.ones([3,100])
  x_mspass.data = z.data + y

In contrast, the following that might seem completely equivalent
raises a ``TypeError`` exception:

.. code-block:: python

  z=Seismogram(x_mspass)
  y=np.ones([3,100])
  x_mspass.data = y + z.data

The reason is that for the right hand side resolves to a
NumPy ``ndarray`` and the left-hand side requires a ``dmatrix``.
Exactly like above, this simpler assignment will fail exactly the same way:

.. code-block:: python

  x_mspass.data = y

The solution is similar to that we used above for `TimeSeries`:

.. code-block:: python

  x_mspass.data = dmatrix(y)

That is, ``DoubleVector`` is replaced by ``dmatrix`` for matrix data.

``dmatrix`` also supports ordinary row and column slices through its NumPy
interface; those slices are returned as ``ndarray`` objects rather than as
another ``dmatrix``.  When a result is assigned back, construct a ``dmatrix``
explicitly, require exactly three rows, and keep ``npts`` equal to the number
of columns:

.. code-block:: python

  window = np.array(x_mspass.data, copy=True)[:, 10:20]
  x_mspass.set_npts(window.shape[1])
  x_mspass.data = dmatrix(window)
  assert x_mspass.data.shape == (3, x_mspass.npts)
