.. _user_manual_introduction:

Introduction
============

MsPASS (Massive Parallel Analysis System for Seismologists) is an open-source
framework for research workflows that process and manage collections of
seismic waveforms.  It brings seismic data objects, database-backed metadata,
processing functions, and parallel execution together behind a Python
interface.  This makes it possible to develop a workflow on a small data set
and then apply the same processing model to a larger collection.  Although
MsPASS is designed around seismology, its audience also includes exploration
geophysicists and researchers in other fields whose sampled-data problems fit
the data models described in this manual.  Because research workflows vary,
the interfaces favor flexible composition instead of prescribing one
processing pipeline.

MsPASS is most useful when a project needs to combine data from different
sources, keep waveform metadata queryable, apply a repeatable sequence of
algorithms, and retain information about data-specific failures.  It is not a
turnkey platform for continuous acquisition, real-time alerting, or seismic
network operations.  It also does not hide every detail of MongoDB, the
selected parallel scheduler, or the system on which a workflow runs.  Users
should validate a workflow and its scientific assumptions locally before
scaling it to a cluster.

How the pieces fit together
---------------------------

MsPASS uses several established technologies, each with a distinct role:

* **Python** is the workflow language and the main user-facing API.  Workflows
  can run in Jupyter notebooks, Python scripts, or non-interactive batch jobs.
* **C++** implements the core seismic data objects and selected numerical
  components, exposed through Python bindings.  The principal objects are
  ``TimeSeries`` and ``Seismogram`` plus ensemble containers.  Each object can
  carry waveform samples, flexible metadata, an error log, and optional
  processing history.  The bound classes and functions are exposed below the
  ``mspasspy.ccore`` package.  See :ref:`seismic data object concepts
  <data_object_design_concepts>`.
* **MongoDB** provides document-oriented storage and queries for metadata and
  the records associated with waveform data.  MsPASS supplies schema and
  database interfaces, but realistic data selection and inspection can still
  require MongoDB query dictionaries.  Basic familiarity with that query
  language is therefore useful.  Start with :ref:`database concepts
  <database_concepts>`.
* **Dask and Spark** provide scheduler and distributed-collection abstractions.
  Dask is the default; Spark is also supported.  MsPASS processing functions
  are designed to compose with their map-and-reduce style of execution.  The
  :ref:`parallel processing guide <parallel_processing>` explains when and how
  to use that layer.
* **Containers** package MsPASS and its runtime dependencies for common
  desktop, cluster, and hosted deployments.  A container simplifies setup,
  but the database, scheduler, storage, and compute resources still need an
  appropriate configuration.  See :ref:`the MsPASS component model
  <mspass_components>` before planning a multi-service deployment.
* **ObsPy** supplies widely used seismological formats and algorithms.  MsPASS
  includes converters and wrappers so that many ObsPy operations can
  participate in MsPASS workflows.  The :ref:`ObsPy integration guide
  <obspy_interface>` describes the conversions and their limitations.

Errors and processing history
-----------------------------

One malformed datum should not necessarily terminate a large parallel job.
MsPASS data objects therefore carry an error log and a live/dead state.
Compatible processing functions can record a problem on the affected object
and mark unusable data dead, allowing downstream operations to skip it without
discarding the diagnostic record.  For live data, the standard
:py:meth:`~mspasspy.db.database.Database.save_data` path automatically writes
nonempty logs to the ``elog`` collection and links them to the saved waveform.
Dead-data records follow a separate burial or cremation policy.  Unhandled or
fatal errors can still stop a workflow; the :ref:`error-handling guide
<handling_errors>` explains the expected behavior and severity levels.

Jupyter notebooks can preserve executable code together with explanatory
narrative.  Processing history is a separate, complementary facility.  Global
history can record algorithms and parameters used by a job, while object-level
history can record the processing path of an individual datum when enabled.
The standard processing wrappers leave object-level history disabled by
default, so a workflow that needs it must enable it explicitly for each
relevant step.  These records can support auditing and reproducibility, but
they do not by themselves capture every dependency or scientific decision in
a project.  See
:ref:`processing history concepts <processing_history_concepts>` for the data
model and current scope.

A recommended learning path
---------------------------

The manual does not need to be read cover to cover.  The sequence below is a
practical starting route; after that, use the tutorials, cross-references, and
topic pages as questions arise.

1. Use the :ref:`desktop quick start <quick_start>` to run a small local
   environment and confirm that its persistent project directory is working.
2. Read :ref:`the MsPASS component model <mspass_components>` and
   :ref:`seismic data object concepts <data_object_design_concepts>` to learn
   the vocabulary used throughout the manual.
3. Continue with :ref:`database concepts <database_concepts>` and the
   :ref:`error-handling guide <handling_errors>` before importing or processing
   a substantial data set.
4. Work through the `MsPASS tutorial notebooks
   <https://github.com/mspass-team/mspass_tutorial>`__ with a small data set,
   using the manual to explore each concept in more depth.
5. After the serial workflow is scientifically correct, read the
   :ref:`parallel processing guide <parallel_processing>` and the relevant
   deployment pages before increasing its scale.
6. Consult the :doc:`Python API <../python_api/index>` while writing workflows.
   The :doc:`C++ API <../cxx_api/index>` is primarily useful when extending the
   core data model or compiled implementation.
