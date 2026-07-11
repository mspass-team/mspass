.. _data_editing:

Data Editing
============

Concepts
--------

MsPASS uses an idea that has been part of seismic-reflection processing since
the earliest days of digital signal processing: *trace editing*.  Trace
editing removes "bad" data from subsequent processing, where "bad" can be
defined by a computed metric or by human editing in a graphical interface.
The decision is boolean: a datum is either accepted or rejected.

MsPASS implements this model with the boolean ``live`` attribute carried by
every MsPASS data object: atomic
:py:class:`TimeSeries<mspasspy.ccore.seismic.TimeSeries>` and
:py:class:`Seismogram<mspasspy.ccore.seismic.Seismogram>` objects, and their
corresponding ensembles.  Calling an object's ``kill`` method marks it dead.
Algorithms should test ``live`` (or ``dead()``) and must not assume that the
sample data in a dead object remain usable.

MsPASS standardizes trace-editing decisions through the
:py:class:`Executioner<mspasspy.algorithms.edit.Executioner>` API.  This
section describes the supplied Metadata-based tests and shows how a custom
metric computed from sample data can feed those tests.

MsPASS Trace Editing Algorithms
-------------------------------

Simple comparison example
+++++++++++++++++++++++++

The supplied editors follow another model borrowed from seismic-reflection
processing: editing based on "header" values.  MsPASS generalizes a header as
the Metadata container on every data object.  The simple editors apply one of
Python's comparison operators (``>``, ``<``, ``<=``, ``>=``, ``==``, or
``!=``) to one Metadata value and a constant threshold.

Suppose a workflow should reject data with an estimated bandwidth below 12
dB (approximately two octaves).  The current SNR estimator stores its result
as a dictionary under the default ``"Parrival"`` Metadata key, so the helper
below promotes the nested ``"bandwidth"`` result to a top-level key before
the editor tests it.  This serial example assumes each Seismogram already has
a measured P-arrival time under ``"Ptime"``:

.. code-block:: python

    from mspasspy.algorithms.edit import MetadataLT
    from mspasspy.algorithms.snr import broadband_snr_QC


    def measure_bandwidth(d):
        d = broadband_snr_QC(
            d,
            use_measured_arrival_time=True,
            measured_arrival_time_key="Ptime",
        )
        if d.live and d.is_defined("Parrival"):
            d["bandwidth"] = d["Parrival"]["bandwidth"]
        return d


    editor = MetadataLT("bandwidth", 12.0)
    query = {}  # Replace with a MongoDB query to process a subset.
    cursor = db["wf_Seismogram"].find(query)
    for doc in cursor:
        d = db.read_data(doc, collection="wf_Seismogram")
        d = measure_bandwidth(d)
        d = editor.kill_if_true(d)


:py:func:`broadband_snr_QC<mspasspy.algorithms.snr.broadband_snr_QC>` has
many other options.  With the arguments above, ``"Ptime"`` is the input
arrival-time key and ``"Parrival"`` is the output subdocument key.  The
:py:class:`MetadataLT<mspasspy.algorithms.edit.MetadataLT>` instance kills a
live datum when its top-level ``"bandwidth"`` value is less than 12.0.

The same operations can be mapped over a Dask bag:

.. code-block:: python

    from mspasspy.io.distributed import read_distributed_data

    editor = MetadataLT("bandwidth", 12.0)
    query = {}
    seisbag = read_distributed_data(
        db,
        query=query,
        collection="wf_Seismogram",
    )
    seisbag = seisbag.map(measure_bandwidth)
    seisbag = seisbag.map(editor.kill_if_true)


The example intentionally omits ``seisbag.compute()`` because these maps are
normally steps within a larger lazy workflow.

.. important::

   Use ``kill_if_true`` explicitly in assignments and parallel maps.  Although
   ``Executioner`` currently defines ``__call__``, that method does not return
   the edited object or forward options such as ``apply_to_members``.
   Consequently, ``d = editor(d)`` and ``bag.map(editor)`` produce ``None``
   results in the current implementation.

All simple comparison editors
+++++++++++++++++++++++++++++

The simple comparison editors share these properties:

#. Each is a subclass of ``Executioner``.
#. Its constructor takes a Metadata ``key``, a constant comparison ``value``,
   and the optional boolean ``verbose=False``.
#. Its ``kill_if_true`` method mutates the input object in place when the test
   succeeds and returns that same object, which makes the method suitable for
   a map operation.  It does not make a data copy.
#. A dead input is returned unchanged.  An input that is not an MsPASS data
   object raises a fatal
   :py:class:`MsPASSError<mspasspy.ccore.utility.MsPASSError>`.
#. A missing comparison key leaves the datum live and unchanged.  Use
   :py:class:`MetadataUndefined<mspasspy.algorithms.edit.MetadataUndefined>`
   when a missing value should instead reject the datum.
#. With ``verbose=True``, every kill adds an ``Informational`` error-log entry
   explaining the decision.  The default is quiet because editing a large
   data set can otherwise create very large error logs.

If ``x`` is the Metadata value and ``a`` is the constant supplied to the
constructor, the available comparisons are shown below.  The two-letter class
suffix is the conventional mnemonic for the comparison (for example, ``LT``
means "less than" and ``GE`` means "greater than or equal").

.. list-table:: Simple Metadata-based edit classes
   :widths: 55 45
   :header-rows: 1

   * - Class
     - Kill test
   * - :py:class:`MetadataGT<mspasspy.algorithms.edit.MetadataGT>`
     - ``x > a``
   * - :py:class:`MetadataGE<mspasspy.algorithms.edit.MetadataGE>`
     - ``x >= a``
   * - :py:class:`MetadataEQ<mspasspy.algorithms.edit.MetadataEQ>`
     - ``x == a``
   * - :py:class:`MetadataNE<mspasspy.algorithms.edit.MetadataNE>`
     - ``x != a``
   * - :py:class:`MetadataLT<mspasspy.algorithms.edit.MetadataLT>`
     - ``x < a``
   * - :py:class:`MetadataLE<mspasspy.algorithms.edit.MetadataLE>`
     - ``x <= a``

Their common constructor signature is:

.. code-block:: python

    Editor(key, value, verbose=False)


Here, ``Editor`` is one of the six classes in the table, ``key`` identifies
the Metadata value ``x``, and ``value`` supplies ``a``.

Atomic data and ensembles
+++++++++++++++++++++++++

Every supplied ``kill_if_true`` implementation accepts
``apply_to_members=False``.  Atomic inputs ignore this option.  For an
ensemble, the default applies the test to the ensemble's own Metadata and can
therefore kill the entire ensemble.  Set ``apply_to_members=True`` to apply
the editor serially to each live member instead:

.. code-block:: python

    ensemble = editor.kill_if_true(ensemble, apply_to_members=True)


Choose this flag deliberately: member Metadata and ensemble Metadata are
different containers.

Existence tests
+++++++++++++++

Unlike classical headers with fixed slots, Metadata is open-ended, so a key
may or may not exist.  MsPASS supplies two explicit existence editors:

* :py:class:`MetadataDefined<mspasspy.algorithms.edit.MetadataDefined>` kills
  a datum when the key exists.
* :py:class:`MetadataUndefined<mspasspy.algorithms.edit.MetadataUndefined>`
  kills a datum when the key does not exist.

Both use the constructor form:

.. code-block:: python

    Editor(key, verbose=False)


``MetadataUndefined`` is especially useful as a prefilter.  If a later
algorithm requires one or more Metadata attributes, it can reject deficient
inputs before they reach code that cannot operate without those values.

Interval comparison
+++++++++++++++++++

A common edit retains values in a range, such as P-wave receiver functions
with epicentral distances between roughly 30 and 100 degrees, or data whose
amplitude-quality metric lies within an acceptable positive range.  The
:py:class:`MetadataInterval<mspasspy.algorithms.edit.MetadataInterval>` class
combines lower- and upper-bound comparisons so that a workflow does not need
multiple simple editors.

An interval has three boolean choices: whether each edge is included and
whether values outside or inside the resulting interval are killed.  Those
three choices have eight possible combinations, all supported by this
constructor:

.. code-block:: python

    MetadataInterval(
        key,
        lower_endpoint,
        upper_endpoint,
        use_lower_edge=True,
        use_upper_edge=True,
        kill_if_outside=True,
        verbose=False,
    )


Let ``a`` be ``lower_endpoint``, ``b`` be ``upper_endpoint``, and ``x`` be
the Metadata value.  ``use_lower_edge=True`` includes ``x == a`` in the
interval, while ``use_upper_edge=True`` includes ``x == b``.  The exact kill
tests are:

.. list-table:: ``MetadataInterval`` behavior
   :widths: 22 22 24 32
   :header-rows: 1

   * - ``use_lower_edge``
     - ``use_upper_edge``
     - ``kill_if_outside``
     - Kill test
   * - ``True``
     - ``True``
     - ``True``
     - ``x < a or x > b``
   * - ``False``
     - ``True``
     - ``True``
     - ``x <= a or x > b``
   * - ``True``
     - ``False``
     - ``True``
     - ``x < a or x >= b``
   * - ``False``
     - ``False``
     - ``True``
     - ``x <= a or x >= b``
   * - ``True``
     - ``True``
     - ``False``
     - ``a <= x <= b``
   * - ``False``
     - ``True``
     - ``False``
     - ``a < x <= b``
   * - ``True``
     - ``False``
     - ``False``
     - ``a <= x < b``
   * - ``False``
     - ``False``
     - ``False``
     - ``a < x < b``

As with the simple comparison editors, an undefined ``key`` leaves the datum
unchanged.  Pair ``MetadataInterval`` with ``MetadataUndefined`` if absence
must also cause rejection.

Defining multiple editors
+++++++++++++++++++++++++

:py:class:`FiringSquad<mspasspy.algorithms.edit.FiringSquad>` applies
multiple ``Executioner`` instances in one ``kill_if_true`` call.  Its
constructor is:

.. code-block:: python

    FiringSquad(executioner_list)


Pass a reusable iterable such as a list or tuple containing only
``Executioner`` instances.  A one-shot generator should not be used: the
current constructor iterates once to validate its contents and again to copy
them.  A non-``Executioner`` element raises a fatal ``MsPASSError``.
``FiringSquad`` itself has no ``verbose`` constructor argument;
configure ``verbose`` on each component editor that should log its kills.

The component editors run in the order supplied.  Once one kills the datum,
the loop stops and later tests are skipped.  Ordering therefore matters when
verbose logs are used: the first failing test is the recorded cause.

``FiringSquad`` also implements ``+=`` to append a test.  For example, this
adds a ``<= 4.0`` test for ``"mad_snr"`` to an existing ``squad``:

.. code-block:: python

    maddog = MetadataLE("mad_snr", 4.0)
    squad += maddog


A ``FiringSquad`` is itself an ``Executioner``, so its input list may contain
another ``FiringSquad``.  It also supports ``apply_to_members=True`` with the
same ensemble semantics described above.

Related Metadata transformations
--------------------------------

Executioners decide whether data remain live; they do not provide general
Metadata mutation.  For renaming or setting keys, constant and two-key
arithmetic, and ordered transformation chains, see :ref:`header_math`.  The
corresponding API includes
:py:class:`ChangeKey<mspasspy.algorithms.edit.ChangeKey>`,
:py:class:`SetValue<mspasspy.algorithms.edit.SetValue>`, the arithmetic
``MetadataOperator`` subclasses, and
:py:class:`MetadataOperatorChain<mspasspy.algorithms.edit.MetadataOperatorChain>`.
For deleting a list of keys, see
:py:func:`erase_metadata<mspasspy.algorithms.edit.erase_metadata>`.

How to implement an extension
-----------------------------

Before writing a custom editor, consider two simpler alternatives:

#. Can the test be expressed as a ``FiringSquad`` of existing editors?
#. If a nonstandard quantity must be computed from sample data, can the result
   be reduced to a small set of values stored in Metadata?  If so, keep the
   unique computation in a normal function and apply the supplied Metadata
   editors afterward, as in the bandwidth example above.

A custom editor that participates in the same API must:

#. subclass ``Executioner``; and
#. implement ``kill_if_true``.

Most implementations also need a constructor that saves the thresholds or
other parameters used by ``kill_if_true``.  Follow the supplied editors for
the wrapper signature, ensemble handling, dead-data handling, return value,
``MsPASSError`` behavior, and optional verbose logging.  In particular, return
the input object after mutating it rather than making an expensive waveform
copy.

Some additional design points are important:

* Metadata-only tests are not required.  A custom editor may compute a metric
  directly from samples, but it must account for the different sample-data
  organizations of ``TimeSeries``, ``Seismogram``, and ensemble objects.
* A compatible editor should validate that its input is one of the supported
  MsPASS data types before relying on methods such as ``kill``.  The helper
  used internally by this module is private; extension code should not treat
  it as a stable public API.
* If the editor supports ensembles, define clearly whether it tests ensemble
  Metadata or individual members and follow the established
  ``apply_to_members`` convention.
* Consider a ``verbose=False`` option and use the inherited ``log_kill``
  method for consistent error-log entries.
* Use the current implementations in ``mspasspy.algorithms.edit`` as working
  examples, and add focused tests for live inputs, dead inputs, missing keys,
  both interval edges where relevant, and ensemble behavior.
