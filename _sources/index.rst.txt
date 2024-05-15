.. MsPASS documentation master file, created by
   sphinx-quickstart on Wed Feb  5 16:31:33 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

MsPASS Documentation
====================

The Massive Parallel Analysis System for Seismologists
is an open source framework for seismic data processing
and management. It has three core components:

* A scalable parallel processing framework based on a
  dataflow computation model.
* A NoSQL database system centered on document store.
* A container-based virtualization environment.

The system builds on the `ObsPy <http://obspy.org>`_
toolkit, with extension built on a rewrite of the
`SEISPP <http://www.indiana.edu/~pavlab/software/seispp/html/index.html>`_
package.

.. .. mdinclude:: ../../README.md

.. toctree::
   :maxdepth: 1
   :caption: Getting Started

   getting_started/quick_start
   getting_started/run_mspass_with_docker
   getting_started/deploy_mspass_with_docker_compose
   getting_started/deploy_mspass_on_HPC
   getting_started/getting_started_overview
   getting_started/deploy_mspass_with_conda_and_coiled

.. toctree::
   :maxdepth: 1
   :caption: Introduction

   user_manual/introduction

.. toctree::
   :maxdepth: 1
   :caption:  Data Management

   user_manual/database_concepts
   user_manual/CRUD_operations
   user_manual/mongodb_and_mspass
   user_manual/normalization
   user_manual/importing_tabular_data

.. toctree::
   :maxdepth: 1
   :caption: Seismic Data Objects

   user_manual/data_object_design_concepts
   user_manual/numpy_scipy_interface
   user_manual/obspy_interface
   user_manual/time_standard_constraints
   user_manual/processing_history_concepts
   user_manual/continuous_data
   user_manual/schema_choices

.. toctree::
   :maxdepth: 1
   :caption: Data Processing

   user_manual/algorithms
   user_manual/importing_data
   user_manual/handling_errors
   user_manual/data_editing
   user_manual/header_math
   user_manual/graphics
   user_manual/signal_to_noise
   user_manual/adapting_algorithms

.. toctree::
   :maxdepth: 1
   :caption: System Tuning

   user_manual/parallel_processing
   user_manual/memory_management
   user_manual/io
   user_manual/parallel_io

.. toctree::
   :maxdepth: 2
   :caption:  FAQ

   user_manual/FAQ
   user_manual/development_strategies


.. toctree::
   :maxdepth: 1
   :caption: Reference Manual

   python_api/index
   cxx_api/index
   mspass_schema/mspass_schema
