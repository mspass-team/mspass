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
   :maxdepth: 2

   user_manual
   tutorial
   reference_manual

.. warning::

    The current version is under development. The APIs are subject to breaking change.


* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
