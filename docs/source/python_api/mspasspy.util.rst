mspasspy.util
=============

Common utility entry points
---------------------------

``mspasspy.util`` contains adapters and workflow helpers that are easy to miss
when browsing by algorithm name.  Start with the entries below before writing
a project-specific conversion or ensemble loop; the generated module reference
that follows lists every public member.

.. list-table:: Common utility entry points
   :widths: 22 48 30
   :header-rows: 1

   * - Task
     - Useful APIs
     - Related guide
   * - Convert between ObsPy and MsPASS
     - :py:func:`~mspasspy.util.converter.Trace2TimeSeries`,
       :py:func:`~mspasspy.util.converter.TimeSeries2Trace`,
       :py:func:`~mspasspy.util.converter.Stream2Seismogram`, and
       :py:func:`~mspasspy.util.converter.Seismogram2Stream`
     - :ref:`ObsPy interfaces <obspy_interface>`
   * - Convert ensembles and streams
     - :py:func:`~mspasspy.util.converter.Stream2TimeSeriesEnsemble`,
       :py:func:`~mspasspy.util.converter.TimeSeriesEnsemble2Stream`,
       :py:func:`~mspasspy.util.converter.Stream2SeismogramEnsemble`, and
       :py:func:`~mspasspy.util.converter.SeismogramEnsemble2Stream`
     - :ref:`Continuous data handling <continuous_data>`
   * - Move between dictionaries, Metadata, and tabular data
     - :py:func:`~mspasspy.util.converter.dict2Metadata`,
       :py:func:`~mspasspy.util.converter.Metadata2dict`, and
       :py:func:`~mspasspy.util.converter.Textfile2Dataframe`
     - :ref:`Importing tabular data <importing_tabular_data>`
   * - Inspect and organize ensembles
     - :py:func:`~mspasspy.util.seismic.number_live`,
       :py:func:`~mspasspy.util.seismic.has_live_data`,
       :py:func:`~mspasspy.util.seismic.ensemble_time_range`, and
       :py:func:`~mspasspy.util.seismic.sort_ensemble`
     - :ref:`Seismic data object concepts <data_object_design_concepts>`
   * - Regularize sampling before ensemble processing
     - :py:func:`~mspasspy.util.seismic.regularize_sampling`
     - :ref:`Arrival-time processing <arrival_time_measurement>`
   * - Clean Metadata and manage dead data
     - :py:class:`~mspasspy.util.Janitor.Janitor`,
       :py:class:`~mspasspy.util.Janitor.MiniseedJanitor`, and
       :py:class:`~mspasspy.util.Undertaker.Undertaker`
     - :ref:`Cleaning Metadata <cleaning_metadata>` and
       :ref:`handling errors <handling_errors>`
   * - Adapt an external function to MsPASS objects
     - :mod:`mspasspy.util.decorators`, especially
       ``mspass_func_wrapper``, ``mspass_method_wrapper``, and the ObsPy
       conversion decorators
     - :ref:`Adapting algorithms <adapting_algorithms>`

converter
---------

.. automodule:: mspasspy.util.converter
    :members:
    :undoc-members:
    :show-inheritance:

db_utils
----------

.. automodule:: mspasspy.util.db_utils
    :members:
    :undoc-members:
    :show-inheritance:

decorators
----------

.. automodule:: mspasspy.util.decorators
    :members:
    :undoc-members:
    :show-inheritance:

error_logger
------------

.. automodule:: mspasspy.util.error_logger
    :members:
    :undoc-members:
    :show-inheritance:

Janitor
--------

.. automodule:: mspasspy.util.Janitor
    :members:
    :undoc-members:
    :show-inheritance:

logging_helper
--------------

.. automodule:: mspasspy.util.logging_helper
    :members:
    :undoc-members:
    :show-inheritance:

seismic
--------

.. automodule:: mspasspy.util.seismic
    :members:
    :undoc-members:
    :show-inheritance:

seispp
------

.. automodule:: mspasspy.util.seispp
    :members:
    :undoc-members:
    :show-inheritance:

Undertaker
----------

.. automodule:: mspasspy.util.Undertaker
    :members:
    :undoc-members:
    :show-inheritance:
