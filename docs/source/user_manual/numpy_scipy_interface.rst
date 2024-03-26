.. _numpy_scipy_interface:

Using numpy/scipy with MsPASS
============================================
Overview
-------------
The `numpy` and `scipy` packages are heavily utilized for scientific computing
in a wide range of fields.   One of the reasons for that is performance.
For a number of reasons that are beside the point, python arrays
have horrible performance.  That is particularly true of linear algebra
operations common in all signal processing algorithms.
All numpy algorithms we've ever tested are orders of magnitude faster than the same
algorithm implemented as a pure python code with python arrays.  Since most
scipy algorithms are built on top of numpy, the same holds for scipy.
Similarly, obspy uses numpy arrays exclusively.   As a result, an early
design constraint in MsPASS was to provide as clean an interface to
numpy/scipy as possible.  However, because we chose to implement our own
core data objects in C++, that presents a disconnect that causes a few
anomalies in the MsPASS API.  This section of the User's Manual
documents these issues and provides guidance for how to effectively mix
numpy/scipy algorithms with `TimeSeries`, `Seismogram`, and ensemble objects.
It does not address a prototype data type we call a `Gather` that can be used
for uniform ensembles.  The `Gather` object is a pure python class that uses
multidimensional numpy arrays to store sample data and is not subject to
the issues addressed in this section.

TimeSeries Data
-----------------
Recall the mspass `TimeSeries` class is used to abstract the concept of
a single channel of fixed sample rate seismic data spanning a given time period.
As such we used the standard signal processing abstraction of storing
the data as a vector of numbers.

Every computing language that has any role in numerical computing defines
a syntax for arrays.  A vector in this context is a one-dimensional array.
A problem with python's builtin array is that it isn't actually at all the
same thing as an array in FORTRAN of C.   Both FORTRAN and C define
an array as a fixed block of memory of a specified length containing
a sequence of numbers of the same type.   (e.g. a C double array of length
10 is a block of :math:`8*10` bytes.)  Numpy performance is achieved by
redefining an array to assume the data are in a contiguous memory block and
using wrappers that allow low-level operations to actually be performed
by compiled C or FORTRAN functions.  We do the same thing in MsPASS
but our data objects do not contain numpy array classes per se, but
we use the same approach to allow low-level operations to be
done in C++.  Because we use the same concept (arrays are contiguous
blocks of memory) the arrays can mostly be used interchangeably, but
not always.

So what is the difference?   If you execute this code fragment in MsPASS:

.. code-block:: python

  import numpy as np
  from mspasspy.ccore.seismic import TimeSeries
  x_numpy = np.zeros(100)
  x_mspass = TimeSeries(100)
  print("Type of numpy object=",type(x_numpy))
  print("Type of mspass data vector=",type(x_mspass.data))

You should get this output:

.. code-block:: python

  Type of numpy object= <class 'numpy.ndarray'>
  Type of mspass data vector= <class 'mspasspy.ccore.seismic.DoubleVector'>

The point is that both define two conceptually similar things that are a
vector of sample data.  In this example
these are stored with the symbols `x_numpy` a `x_mspass.data`.
The print statements emphasize they are
different data types.  They are not, however, an apples and oranges
kind of comparison but more of a Jonathan versus Gala apple comparison.
Python is very cavalier about type.  Many sources call this "duck typing"
from the cliche "if it walks like a duck and quacks like duck it must be duck".
Because of "duck typing" we can, in most cases, interchange the use of the
two classes `ndarray` and `DoubleVector`.
For example, you can nearly always send `DoubleVector` data to a
numpy or scipy function and it will work just find.  Here are a few
examples:

.. code-block:: python

  import numpy as np
  import scipy
  # Setting data to nonzero values - not essential but otherwise
  # trivial zero vector will cause problems for the log example
  for i in range(x_mspass.npts):
    x_mspass.data[i]=float(i+1)
  x = np.log10(x_mspass.data)
  y = np.sqrt(x_mspass.data)
  z=np.cos(x_mspass.data)
  yh = scipy.signal.hilbert(x_mspass.data)

That works because the `DoubleVector` is what numpy's documentation
calls "array like" (a duck type concept).  In fact, any numpy or scipy
function that accepts an "array like" argument can accept the `data`
member of a `TimeSeries` and work.   In reality, `DoubleVector` is
a C++ "vector container" that in C++ would be declared as:

.. code-block:: C

  std::vector<double> x;

In MsPASS we use stock "bindings" from pybind11 that allow us to do
this magic.  The real magic is that in most cases no copying of the
array data is required.   (For C programmers the bindings use
a pointer (address) not a copy.)

This magic works for most, but not necessarily all, vector operators that
mix `DoubleVector` and `ndarray` types.  e.g. with the symbols above defined
all of the following will work and do what is expected:

.. code-block:: python

  z=x_numpy-x_mspass.data
  z=x_mspass.data+x_numpy
  x_mspass.data -= z
  z=x_numpy*x_mspass.data  # a peculiar numpy operation - not a dot product
  x_mspass.data *= 10.0

The main thing to avoid is an assignment where the left hand side
and right hand side resolve to different types.   For instance, either of
the following will fail with a `TypeError` exception if you try to run them:

.. code-block:: python

  z=np.ones(100)
  x_mspass.data = x_numpy
  x_mspass.data = x_numpy + z

The solution is to use a python version of a type cast to a `DoubleVector`:

.. code-block:: python

  x_mspass.data = DoubleVector(x_numpy)
  x_mspass.data = DoubleVector(x_numpy + z)

That is necessary because in both cases the right hand side is an
`ndarray`.  The construct used is a call to the bindings of a copy constructor
for the C++ std::vector container so it seems to come at the cost of a memory copy.
On the other hand, that is orders of magnitude faster than a python loop to do the
same operations.

Be aware that if you mix types in a vector operation you can get some surprising
results.   For example, the following code will generate a `TypeError`
exception:

.. code-block:: python

  ts=TimeSeries(100)
  z=np.ones(100)
  x_mspass.data = z + ts.data

while simply reversing the order of `z` and `ts.data` like this

.. code-block:: python

  ts=TimeSeries(100)
  z=np.ones(100)
  x_mspass.data = ts.data + z

Works and does set `x_mspass.data` to the vector sum of the two symbols
on the right hand side.  The reason is that the + operator in python
seems to resolve the type to the type of the leftside of the + operator.
Why is "deep in the weeds", but the best advice is to avoid mixed type
operations if possible.  The more robust way to write the above is this:

.. code-block:: python

  ts=TimeSeries(100)
  z = np.ones(100)
  x = z + np.array(ts.data)
  x_mspass.data = DoubleVector(x)

Finally, users who are matlab fans may like to write python code using
the vector ":" operator.  For example, with numpy operations like the
following can be used in a typical numpy algorithm:

.. code-block:: python

  z=np.ones(100)
  x=z[5:10]
  print(len(x))

That will print "5" - the length of the subvector extracted in x.
The : operator DOES NOT work with a `DoubleVector`.  If you have an
algorithm using the colon operator, do the vector operations with
numpy and then copy the result into the `TimeSeries` data member.
An important warning about any such assignments to `TimeSeries.data` is you
MUST be sure the size of the vector on the right hand side is the
same size as the left hand side.   Consider this segment:

.. code-block:: python

  ts.data=TimeSeries(100)  # creates data vector 100 samples long
  z=np.ones(100)
  x=z[5:10]
  ts.data = DoubleVector(x)
  print(len(ts.data))

The output of this is "5" showing that, as expected, ts.data is replaced
by the subvector x.  The BIG PROBLEM that can cause is it will create an
inconsistency in the size set by the `npts` attribute of `TimeSeries`.  The
correct solution for the above is this minor variant calling the `set_npts` method
of `TimeSeries`

.. code-block:: python

  ts.data=TimeSeries(100)  # creates data vector 100 samples long
  z=np.ones(100)
  x=z[5:10]
  ts.set_npts(5)
  ts.data = DoubleVector(x)
  print(len(ts.data))

Seismogram Data
~~~~~~~~~~~~~~~~~
MsPASS uses a different data type to hold data from three-component sensors.
The motivation is described in the section titled
:ref:`Data Object Design Concepts<data_object_design_concepts>`.
The result is that the array holding the sample data is a matrix
instead of a vector.  Some implementation details of note are:

1.  In MsPASS the matrix has 3 rows and `npts` columns.  That means the
    rows of the matrix an be extracted to create a `TimeSeries`
    subset of the data while the columns are single "vector" samples
    of ground motion.
2.  Currently MsPASS uses a simple, lightweight matrix class to store the sample
    data called :class:`mspasspy.ccore.utility.dmatrix`.  A key reason we
    made that choice was control over methods defined for the class.  A
    case in point is the `shape` attribute that we will see momentarily
    is important for working with numpy.
3.  For performance `dmatrix` is implemented in C++.   The class has methods
    for all standard matrix operations with python bindings for those operator.
    The corollary to that statement is that when working with `Seismogram` data
    use the native operators when possible.
4.  A `dmatrix` stores the array data in a contiguous block of memory
    in what numpy would call "FORTRAN order". ("C order" is reversed.)
    FORTRAN stores a matrix as a contiguous block of memory with the
    row index "varying fastest".  Exchanging the sample data stored
    this way with numpy is like that with the std::vector used in
    `TimeSeries` but the exchange uses a pointer to the first sample of the
    contiguous block of memory held by an instance of a `dmatrix`
    class referenced with the symbol `Seismogram.data`.
5.  We use a variation of the same "magic" in the pybind11 binding code
    that allows the `Seismogram.data` matrix to interact cleanly with
    numpy and scipy functions that require a matrix.  Like the scalar
    case there are impedance mismatches, however, that can complicate
    that exchange. We discuss these below.

Almost all of what was discussed above about using `TimeSeries`
arrays with numpy are similar.  For example, although the result is
meaningless, you can run the following code snippet with
MsPASS and it will run and produce the expected result:

.. code-block:: python

  import numpy as np
  from mspasspy.ccore.seismic import Seismogram
  x_mspass =Seismogram(100)
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
  z=Seismogram(x_mspass.data)
  x_mspass_data = z + y

As with `TimeSeries` at mismatch will occur if the operation
yields a type mismatch.  For example, the following minor variant of
above will run:

.. code-block:: python

  z=Seismogram(x_mspass)
  y=np.ones([3,100])
  x_mspass.data = z.data + y

In contrast, the following that might seem completely equivalent
will raise a `TypeError` exception:

.. code-block:: python

  z=Seismogram(x_mspass)
  y=np.ones([3,100])
  x_mspass.data = y + z.data

The reason is that for the right hand side resolves to a
numpy "ndarray" and the left hand side is a `dmatrix`.
Exactly like above, this simpler assignment will fail exactly the same way:

.. code-block:: python

  x_mspass.data = y

The solution is similar to that we used above for `TimeSeries`:

.. code-block:: python

  x_mspass.data = dmatrix(y)

That is, `DoubleVector` is changed to `dmatrix`.

Finally, the warnings above about the ":" operator with `TimeSeries` apply equally
to `Seismogram`.   If you write a python code with the color operator do
all those operations with numpy arrays and use the `dmatrix` copy constructor
to load the result into the object's `data` array.   The warning about
making sure the `npts` member attribute is consistent with the result
applies as well.   The only thing to remember with a `Seismogram` is that
`npts` is the number of columns of the data matrix, not the total number of
samples.
