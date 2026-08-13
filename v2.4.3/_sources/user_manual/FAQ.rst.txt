.. _FAQ:

Frequently Asked Questions (FAQ)
================================

Use these questions to find the next guide for your task.  The answers are
intentionally brief; follow the links for prerequisites, procedures, and
details.

Getting Started
---------------

Where should I begin if I have never used MsPASS?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Follow the :ref:`desktop quick start <quick_start>` to launch a first local
session, then use the :ref:`User Manual introduction
<user_manual_introduction>` for a guided tour of the main concepts.

Where can I find complete, worked examples?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After completing the :ref:`desktop quick start <quick_start>`, work through
the `MsPASS tutorial notebooks
<https://github.com/mspass-team/mspass_tutorial>`__ with a small data set.

Choosing a Runtime
------------------

Which runtime should I use for a first local session?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Start with the :ref:`single-container Docker guide
<run_mspass_with_docker>`.  If you prefer a graphical interface, see the
:ref:`optional MsPASS Desktop launcher <mspass_desktop>`.

When should I use Docker Compose?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the :ref:`multi-container Docker Compose guide
<deploy_mspass_with_docker_compose>` when you need the main services in
separate containers on one Docker host.

When should I install MsPASS with Conda?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Read the :ref:`Conda installation guide <deploy_mspass_with_conda>` when you
need a local environment for development or a non-container installation.

Where should I start for a cluster or hosted environment?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Read the :ref:`virtual-cluster concepts <getting_started_overview>` before
using the :ref:`HPC deployment guide <deploy_mspass_on_HPC>` or the
:ref:`EarthScope GeoLab guide <deploy_mspass_on_geolab>`.

Data and Database Concepts
--------------------------

What objects represent waveform data in MsPASS?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :ref:`seismic data object concepts <data_object_design_concepts>` page
introduces the core data and metadata containers used throughout MsPASS.

How does MsPASS organize and access stored data?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Begin with :ref:`database concepts <database_concepts>`, then use the
:ref:`CRUD operations guide <CRUD_operations>` for the create, read, update,
and delete interfaces.

What database schema should I use?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :ref:`schema selection guide <schema_choices>` explains how the choice
depends on whether a workflow is exploratory or moving toward production.
For collection and attribute definitions, use the :doc:`MsPASS schema
reference <../mspass_schema/mspass_schema>`.

How do I import waveform data and tabular metadata?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Start with the :ref:`waveform-data import guide <importing_data>` and use the
:ref:`tabular-data import guide <importing_tabular_data>` for metadata stored
in formats such as CSV files or pandas data frames.

Processing and Troubleshooting
------------------------------

How does MsPASS handle continuous data?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See :ref:`continuous data handling <continuous_data>` for the data model and
workflow patterns used for long, segmented records.

What should I do when data or an algorithm fails?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the :ref:`error-handling guide <handling_errors>` to understand error
logs, live and dead data, and failure behavior in processing workflows.

Where can I find the available processing algorithms?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the :ref:`algorithm quick reference <algorithms>` to find operations on
MsPASS seismic data objects.  See :ref:`adapting an existing algorithm
<adapting_algorithms>` when an external algorithm needs a MsPASS interface.

Where do I look up a class, function, or module?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the :doc:`Python API reference <../python_api/index>` for the current
Python module hierarchy and the :doc:`C++ API reference <../cxx_api/index>`
for the core C++ interfaces.

Scaling and Development
-----------------------

When should I parallelize a workflow?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Validate the serial workflow first, then read the :ref:`parallel processing
guide <parallel_processing>` before choosing a scheduler or increasing scale.
For resource bottlenecks, continue to the :ref:`memory-management
<memory_management>`, :ref:`I/O <io>`, and :ref:`parallel-I/O <parallel_io>`
guides.

How do I develop a new workflow from scratch?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :ref:`workflow development guide <development_strategies>` presents a
progression from small Python workflows to more complex algorithm adapters.
