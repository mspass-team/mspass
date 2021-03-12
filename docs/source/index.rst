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

   getting_started/run_mspass_with_docker
   getting_started/deploy_mspass_with_docker_compose


.. toctree::
   :maxdepth: 1
   :caption: User Manual

   user_manual/introduction
   user_manual/data_object_design_concepts
   user_manual/time_standard_constraints
   user_manual/obspy_interface
   user_manual/database_concepts
   user_manual/CRUD_operations
   user_manual/handling_errors
   user_manual/graphics
   user_manual/processing_history_concepts
   user_manual/adapting_algorithms


.. toctree::
   :maxdepth: 1
   :caption: Tutorial

   tutorial/metadata_pf


.. toctree::
   :maxdepth: 1
   :caption: Reference Manual

   python_api/index
   cxx_api/index
   mspass_schema/mspass_schema

.. warning::

    The current version is under development. The APIs are subject to breaking change.
