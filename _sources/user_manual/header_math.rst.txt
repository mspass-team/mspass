.. _header_math:

Header (Metadata) Math
==========================
Concepts
------------
Anyone familiar with seismic reflection data processing has seen the
common need for what is sometimes called "header math".   That term in the
seismic reflection world is inherited from the days of tape-based processing
where the processing workflow was driven by "header" attributes that were
originally data stored in specific (binary) slots defined in tape formats like
SEGY.   The attributes were "header" values because the slots holding these
attributes always preceded the sample data.   The MsPASS framework
uses a more general container that we call "Metadata" to hold attributes
that might have been stored as "header data" in older frameworks.
A useful conceptual model to remember in MsPASS is that the Metadata
container is a generalized header.

With that review, any reader who has used a seismic reflection processing
system knows exactly why "header math" is a frequent requirement in
a workflow.   The general problem is that it is often necessary to
compute other attributes using the contents of one or more other
(Metadata) attributes.   A common example in reflection processing
of simple surveys is geometry formulas that
can be used to compute line geometry attributes from other attributes
like survey flag numbers.  Another from passive array processing is
a need to compute some combined score of signal-to-noise ratio based on
a linear combination of single metrics.

The collection of edit operators covered in this section were implemented
to cover the main arithmetic operations to do "header math".  They have the
advantage of being expressed through a standardized API that allows a chain
of operation to be computed that can implement fairly elaborate
calculations.  The user should recognize that the main advantage of using
these operators over a custom-code python function
(see the *Alternative* subsection at the end of this document) is robustness
and integration of the steps into the history and error logging
concepts of MsPASS.

Unary Operators
---------------------
The first set of operations are "unary" because they implement
all the standard unary arithmetic operations in python.
By that we me operations like `a+=b` which adds the value of b to a.
The distinction is that the operators automatically (and robustly) fetch and update the
Metadata attribute on the left hand side of that operation.

It is likely clearest to show an example of how this is used.
Suppose we wanted found the `calib` attribute in our entire data set
was now off by a factor or 2 because some custom algorithm did that and
made all the amplitudes wrong by a factor of 2.  We could define an
operator to do that with this construct

.. code-block:: python

  import mspasspy.algorithms.edit as mde 
  myop = mde.Multiply("calib", 2.0)
  # parallel example with dask - assumes data are in dask bag mydata
  mydata = mydata.map(myop.apply)


The call to ``mde.Multiply`` is a call to the constructor for the python
class with the name ``Multiply``.  The constructor for Multiply and all
the other unary operators have two required arguments.  arg0 is the Metadata key
to which the operator is to be applied (in this case "calib") and arg1
is the constant value that is to be applied.  The class name defines the
operation that is to be performed.  In the example that means
``calib *= 2.0``.

All the unary operators use exactly the same API and can be used the
same way but for different arithmetic operations with a constant.
The following table summarizes the available operators:

.. list-table:: Unary Metadata Operators
   :widths: 50 50 50
   :header-rows: 1

   * - Name
     - Python op
     - Constructor
   * - Add
     - +=
     - ``Add(key, const)``
   * - Subtract
     - -=
     - ``Subtact(key, const)``
   * - Multiply
     - *=
     - ``Multiply(key, const)``
   * - Divide
     - /=
     - ``Divide(key, const)``
   * - IntegerDivide
     - //=
     - ``IntegerDivide(key, const)``
   * - Mod
     - %=
     - ``Mod(key, const)``

As can be seen the class name is a word describing the arithmetic
operator.  If you are not familiar with the python operator symbols
meaning, see any book or online source on python fundamentals.

Binary Operators
--------------------------
The binary operators are like the unary operators but they define all
operations that are python binary operators.  By that we mean any
operation that can be case as:  ``c = a op b`` where ``op`` is one of the
standard arithmetic operator symbols:  ``+``, ``-``, ``*``, ``/``, ``//``, and ``%``.
The distinction from normal usage is that the operator has to first cautiously
fetch ``a`` and ``b`` from Metadata, apply ``op``, and then set the value ``c`` to
a value associated with a Metadata key associated with the left hand side
for the operator.  Like the unary operators the binary operators share
a common constructor signature:

.. code-block:: python

    op(keyc,keya,keyb)

where ``op`` is the name for the operation (see table below), keyc is the
key to set for the output of the operator, while keya and keyb are the keys used
to fetch a and b in the formula ``c = a op b``.  keyc can be the same as either
keya or keyb but be aware if it is that the contents of that key will be
overwritten.

The names for the ``op`` variable above are illustrated in the table below.
They are essentially the same as the unary operators with a "2" added to the
name.

.. list-table:: Binary Metadata Operators
   :widths: 50 50 50
   :header-rows: 1

   * - Name
     - Python op
     - Constructor
   * - Add2
     - \+
     - ``Add(keyc, keya, keyb)``
   * - Subtract2
     - \-
     - ``Subtact(keyc, keya, keyb)``
   * - Multiply2
     - \*
     - ``Multiply(keyc, keya, keyb)``
   * - Divide2
     - \/
     - ``Divide2(keyc, keya, keyb)``
   * - IntegerDivide
     - //
     - ``IntegerDivide(keyc, keya, keyb)``
   * - Mod2
     - %
     - ``Mod(keyc, keya, keyb)``

Non-arithmetic Operators
-------------------------------
There are currently two additional operators in the same family as the
arithmetic operators discussed above.

First, there is an operator to change the key assigned to a Metadata attribute.
The constructor has this usage:

.. code-block:: python

    op = ChangeKey(old, new, erase_old=True):

The apply method of this class will check for the existence of data with the key
``old`` and redefine the key to the valued defined by the `old` (positional) argument
passed to the constructor.   The ``erase_old`` argument defaults to True.  If set
False ``old`` will be set with a copy and ``new`` will be retained.

The second is an operator to set a Metadata attribute to a constant value
saved in the operator class.  The value can be any valid python type so
this operation may or may not be an "arithmetic" operation.

The constructor for this class has this usage:

.. code-block:: python

    op = SetValue(key, const):

The apply method of this operator will set a Metadata attribute with the
name defined by ``key`` to the constant value set with ``const``.

Combining operators
------------------------
We define a final operator class with the name ``MetadataOperatorChain``.
As the name suggests it provides a mechanism to implement a (potentially complicated)
formula from the lower level operators.  The class constructor has
this usage:

.. code-block:: python

    opchain = MetadataOperatorChain(oplist)

where ``oplist`` is a python list of 2 or more of the lower level operators
described above.

For example, here is a code fragment to produce a calculator that will
compute the midpoint coordinates from Metadata attributes rx,ry,sx, and sy
and set them as cmpx, cmpy for x and y coordinates respectively:

.. code-block:: python

  import mspasspy.algorithms.edit as mde
  xop1 = mde.Add2("cmpx", "rx", "sx")
  xop2 = mde.Divide("cmpx", 2.0)
  yop1 = mde.Add2("cmpy", "ry", "sy")
  yop2 = mde.Divide("cmpy", 2.0)
  opchain = mde.MetadataOperatorChain([xop1,xop2,yop1,yop2])

The opchain contents can then be passed to a parallel map operator as in
the simpler example above.   This operator computes and sets the following:

.. code-block:: python

  cmpx = (rx + sx) / 2.0
  cmpy = (ry + sy) / 2.0


Common Properties
--------------------
All of the operations defined in this set of operator classes could be
hand coded as needed.  The main thing they give you over a "roll you own"
implementation is automatic handling of the following standard features of
the MsPASS framework:

* All handle error consistently using the ErrorLogging mechanism of MsPASS
  data objects.
* All behave identically on some standard error situations.  There are three
  users need to be aware of.  (1) If a key-value that the operator needs to fetch from
  Metadata is not defined the operator will kill the datum missing and
  log a standard message.  (2) if
  the value extracted fails on the arithmetic operation the datum will again
  be killed with a standard message posted to the elog attribute of the
  data object. (3) If the operator receives a datum that is not a MsPASS
  data object the operator will throw a MsPASSError object marked Fatal.
* All operators handle Ensembles in a consistent manner.   Editing Metadata
  for an Ensemble object has an ambiguity because Ensemble objects often
  have attributes independent of the members (e.g. a common source gather
  may only have the source coordinates in the ensemble container.)  To
  handle this all the apply methods have a common, optional argument
  `apply_to_members`.   When set True the operator will automatically
  apply the operation to each member of the ensemble in a simple, serial loop.
  When false the operation is applied to the ensemble metadata container.
* All the operators have wrappers to optionally enable the object-level
  history mechanism for each datum processed.


Best Practices
------------------

1. It is important to be aware of the consistency of the Metadata attributes
   for a data set before running these operators.  They will dogmatically kill
   data when required attributes are missing.   If your data set has a lot of
   missing metadata you need to do the calculations these operators will
   kill every datum that is lacking.
2. It is far to easy to kill every datum in your data set if you read
   data by ensembles and fail to use the `apply_to_members` switch correctly.
   With the default value of False if you mix up the names for fields you
   set in the ensemble container and which you load with each atomic data
   object you can easily kill every ensemble in the data set.  As always it
   is prudent to run tests with a restricted portion of the data to verify
   the operation does what you think it will before releasing a workflow
   on a huge data set.
3. When you are aware that some data have deficient metadata attributes
   that are required for a calculation, it is prudent to first pass the
   workflow through one related Executioner classes to "kill" data that
   lack the required attributes.
4. We have found that a chain of ``ChangeKey`` operator is almost always a
   far faster way to repair database name errors than to run
   one-at-a-time transactions with MongoDB.   Millions of update transactions
   with MongoDB can (literally) take days to complete but the same operation
   done inline with a string of ``ChangeKey`` operations produces near zero
   overhead on any reasonable processing job.  The same is true if the
   goal were to compute new attributes from all documents defining a
   large data set.  It can be very slow to compute such attributes from
   a serial read-compute-update pure database compared to using the
   operators described in this section as a part of the workflow.
