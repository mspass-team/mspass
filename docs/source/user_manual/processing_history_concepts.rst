.. _history_concepts:

Processing History Concepts
========================

Global versus object level history
---------------------------------------

The mechanism we use to preserve processing history in MsPASS makes an
assumption that the workflow can be broken down into a series of
black boxes (algorithms) that input one or more atomic data objects and emit
one or more (potentially different) *atomic data objects*. MsPASS currently defines
two object types as atomic:  *Seismogram* and *TimeSeries*.  By *atomic* we mean
they are indivisible.  This is in contrast to *Ensemble* objects that are
collections of atomic objects grouped by an unspecified method.

With this conceptual model (the concept behind this design),
preserving processing history has two components.

1. *Global history* is the list of *unique definitions* of algorithms used
   in a workflow.   The *unique* qualifier is necessary because most
   algorithms have one to thousands of numbers/strings that define a unique
   behavior.   To keep the discuss concise in this document we define a
   *parameter* as data used by an algorithm that is NOT one of the the atomic
   data to MsPASS.   A parameter can thus be something as simple as a
   single integer or real number, or as elaborate as any python object (e.g.
   a dict or list container).  We assume the behavior of a particular algorithm
   can be uniquely defined by a unique name assigned to the algorithm and
   a particular set of parameters used to control it's behavior.   The
   simplest example is a function call with the algorithm set to the name
   of the function and the parameters as a set of simple real and integer
   inputs.   More complicated examples may require some more elaborate recipe
   with hierarchies and/or auxiliary data inputs.   The conceptual model
   here puts no restriction on what the parameters are but treat that as
   an implementation detail.  Any algorithm that aims to preserve processing
   history must define a mechanism to load and store the parameters that
   define a unique instance of that algorithm.

2. *Object level history*.  To fully recreate the processing history of a
   processed data set one needs to retain the chain of processes that operated
   on what data to produce the final output.  In every seismic reflection processing
   system we are aware of the problem is reduced to what we are calling global
   history.   Traditional labels displayed the chain of processing modules
   applied to produce a final stacked section on a side bar on paper plots.
   Digital forms largely replicated that model as a button to display the same
   information on a screen.  In designing MsPASS we recognized there was a need
   for object level history from at least two sources:  (1) python is a much
   richer language than traditional, custom job control languages used by
   processing packages and (2) some machine learning algorithms are iterative
   at the object level.   On the other hand, although the need object-level history
   is clear a naive implementation is potentially problematic in two
   ways:  (1) memory bloat, and (2) performance.  We created a mechanism for
   preserving object level history that is lightweight by preserving
   only the minimum information needed to reconstruct a tree structure that
   defines that tree.  We made the mechanism fast by implementing the
   mechanism in C++ using an std::multimap container for which insertion
   is about as fast as possible.  The user interface hides the implementation
   details.   You as a user need only consult the API references to a
   object we call *mspass.ProcessingHistory*.     (NEED A LINK HERE TO python
   documentation)  User's need only recognize that *Seismogram* and *TimeSeries*
   objects (THESE ALSO NEED TO BE LINKS TO PYTHON API PAGES) have a
   ProcessingHistory component.  Algorithms can and should at least allow
   the option of registering themselves through this mechanism.   All
   algorithms supported in MsPASS have an option to enable object level
   history.   NEED A FEW MORE SENTENCES HERE WITH A LINK TO PAGE WITH THE TOPIC
   OF WRITING YOUR OWN PROCESSING MODULE EXTENSION.  THAT DOCUMENT CAN AND
   SHOULD CONTAIN A SECTION ON HOW TO SET UP THE HISTORY MECHANISM.
