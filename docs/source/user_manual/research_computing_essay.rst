.. _research_computing_essay:

MsPASS as a Research Computing Framework
============================================
An essay by *Gary L. Pavlis*
--------------------------------

Overview
-------------
MsPASS was designed as a framework to support research computing.
This document is a small essay on what exactly that means and how MsPASS
was designed to support research computing.
A key premise of my essay is that few outside the scientific computing
community understand what research computing is.   Furthermore,
there are many misconceptions within the field of seismology itself
that have created problems for decades.  I hope this essay will be helpful
to reduce misconception people may have about how a piece of software
like MsPASS can aid their work.

Readers should recognize my perspective on this problem that give me
some credentials worth highlighting. I wrote my first computer program almost
50 years ago.  Computational seismology was a
focus of my career as I lived through the transition in earthquake seismology
from analog to digital data brought on by the emergence of the IRIS consortium.
The changes in the science of seismology brought on by this revolution in
data are stunning to realize from this perspective.   Stunning advances
in computer technology drove this revolution, but at an uneven rate.
The technology that drove the need for MsPASS is the fact that about 20 years
ago a single CPU computer became something you would find only on seismic instruments
or low-end computers.   The state-of-the-art in HPC today is thousands+ of
"cores" that can be applied to computational problems using clusters of
rack-mounted machines linked by some high-speed network.   The disconnect that
caused in our community is that prior to MsPASS the only affordable seismology
software packages that could
exploit modern clusters were specialized applications tuned to a particular
problem (e.g. SPECFEM simulation) or enterprise solutions for real-time network
data acquisition (e.g. Antelope uses multithreaded code extensively for
their data acquisition programs). The old tried and true SAC program today
is a complete dinosaur as it has no capabilities at all to exploit
multicore let alone a full cluster with many nodes.  Obspy was a step forward
but it currently lacks two features that shaped the design of MsPASS:
(1) obspy has no direct
support for multicore or cluster computation and (2) obspy has no integrated database
for managing very large data sets.

This essay is intended to be instructional.  It is designed mainly for
students to help them see the big picture of how to attack
a data processing problem in seismology with MsPASS.   The first
section is an extended discussion of what research computing is and
why any student needs to understand why it is not at all the same thing
as running an "app" like Microsoft Word, Excel, or Adobe Illustrator.
The second section discusses strategies you as a student might
need to consider depending on where you come into a particular problem.
That is, some ideas are frontier and you may need to more-or-less start
from scratch.   Other science problems you may be facing have
partial solutions that range from needing to parallelize an existing
algorithm to tuning an existing workflow that is too slow to
work for the scale of problem you need to address.  These
are developed as a progression for this document, but you may be
entering the problem in the middle.  You will need to judge where you
are to design a strategy to solve the problem you need to address.

Research versus Enterprise Software
--------------------------------------
First, the reader needs to understand what I mean by these terms.
You can find a lot of resources on this topic with a web search.
For other perspectives here are a few I found that are useful supplements
to what I'm writing here:
- `This<https://www.davideaversa.it/blog/research-code-vs-commercial-code/>`__
  Interesting blog entry.
- A Wikepedia entry on `Enterprise Software<https://en.wikipedia.org/wiki/Enterprise_software>`__
- An online book on research coding by `Mineault<https://goodresearch.dev/index.html>`__
  (see that page for a proper citation and doi)

None of the above sources, however, address a fundamental issue all readers
need to understand.   What I mean by "research code" is a continuum of
software from a one-up solution you should never ever try to reuse, to
the kinds of things professors too often inflict on their students
(Something I've sometimes heard with the colorful label
"the dusty FORTRAN deck of punch cards".),
to polished code that runs (mostly) flawlessly but is not bullet proof.
The current situation has evolved over my lifetime.   In the 1970s when I
first encountered computer programming all programs scientists used where
"research code".   The only exception were system tools like the
core operating system code, FORTRAN compilers, and scientific
computing libraries.   That evolved with
time as the toolbox changed.  The emergence of the unix operating system
started a trend that still continues to build research code from
building blocks of increasing complexity.   Modern python packages
like MsPASS are assembled from "Enterprise" components.  That is,
a tacit assumption we made in MsPASS was that key components called MongoDB,
pyspark, dask, docker, and jupyter could be trusted to run with
few if any bugs.

What does "Enterprise" mean then?   You can find lots of definitions
on the web, but a practical statement is a repeat of the last sentence of
the previous paragraph:  Enterprise software can be trusted to run with
few if any bugs.  The fundamental difference is such software is
engineered to accomplish a well defined purpose and is throughly
tested with modern software engineering practice to minimize bugs and
(sometimes) optimize performance.   Like any product how well software
is engineered and tested is dependent on the producer.  Often, but
not always, the reliability of a product depends upon the number of users
of that software.   That is particularly true of open-source projects
where feedback from a user community and long-term support for
developers is essential to keep the product from getting stale.
At the time of this writing we cannot honestly claim that MsPASS is
yet "Enterprise" class software.  It is, however, a long way from a
prototype and can get better only by feedback from user's like you
as we work to improve "the product".

what is MsPASS then?  It is a "framework" for supporting research computing.
That means, it won't do everything for you but provides a set of tools
to help you solve computational problems in seismology that would be
much harder otherwise.   There is an analog in seismology history.
In the 1970s the unix shell revolutionized seismology data processing
by providing a rich environment for assembling data processing workflows
from small "unix filters".   There are entire systems built on this
concept. For example, `Seismic Unix<https://cwp.mines.edu/software/>`__
is a seismic refletion processing system
using unix pipes to move data between processing modules.   Another is the
now largely defunct "AH" system developed at Lamont in the late 1980s.
AH was more-or-less the equivalent of
seismic unix for earthquake data processing.  MsPASS is like these older
packages in that it provides the glue to build a data processing
workflow from pieces supplied with the framework and to make it possible
to develop your own special tool that can easily plug into the framework.
The aim is make it easier for you as a user to advance the science by
not having to continually "reinvent the wheel" for core components
like the database engine, support for parallel processing, and
algorithms that are well established (e.g. most of the algorithms of
SAC and all the obspy algorithms are implemented in MsPASS.).

Solving Computational Seismology Problems with MsPASS
-------------------------------------------------------
Experimental Early Work
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You have an idea for a new algorithm to extract information
from with seismology data.  Where do you start?   Some starting points to consider are:

-  Rarely do any such ideas come from nowhere, but like all science
   your idea builds on previous work.   Are there implementations you
   can build on by authors of key papers?  If so,
   contact that authors to see if they would share their code with you.
   If there is nothing, you have to build on the theoretical background
   of the paper and start from scratch.
   If you get something from the authors,
   you will need to decide how it might be used
   to get you started.  In the best case it is something you can
   experiment with to better understand the problem.   In the worst
   case you can treat it as a prototype you use only to guide development
   of your own prototype.
-  Remember any new idea may fail completely or, more commonly, be subject
   to a lot of dead ends that require major revisions of your prototype
   code.  That is why so much code you find from colleagues is full of patches and
   very awkward and/or confusing constructs.  (In an early USArray
   data processing course Ed Garnero coined a very descriptive phrase
   to describe such research code:  duct taping.)  All research code
   grows from a set of ideas patched together to form a prototype solution
   you have enough confidence in being correct to publish the results.  That is normal
   and inevitable as you gain understanding of the problem.
-  The first step in prototyping any algorithm is usually to run it against
   a simulation where the answer is known and/or predictable from a
   related calculation.  The rich collection of scientific computing
   algorithms in numpy and scipy are likely your friend in this process.
   Be particularly aware that the MsPASS `TimeSeries` object `data`
   attribute can almost always be passed to numpy or scipy functions
   that require a vector of numbers as input.   A good example for
   using MsPASS to generate simulation data can be found in
   our `deconvolution tutorial<https://github.com/mspass-team/mspass_tutorial/blob/master/notebooks/deconvolution_tutorial.ipynb>`__
   The point is a first step is likely to require writing a python
   program to generate simulation data you can use for initial testing.
-  Test your idea with a small data set with a serial loop before creating
   a parallel workflow.  This is a type example of an axiom of computing I
   first heard decades ago from a source I've long since forgotten:
   "Make it work before you make it fast".   Python is a great language
   for prototyping.  An important advantage of using a serial workflow
   is that, at present anyway, it is much easier to debug any serial
   code than almost any parallel code.   In particular, serial codes
   can normally be debugged with one of the rich collection of IDEs
   (Integreated Development Environnment) available in the python
   ecosystem (e.g. Spyder, Pycharm, or Visual Studio).   The dark side
   at this  point, however, is that to do so you will need to build a local
   copy of MsPASS.  Instructions for doing so are found
   `here<https://github.com/mspass-team/mspass/wiki/Compiling-MsPASS-from-source-code>`__
   but the process is currently not trivial.   It is, however, much easier
   if you do not need to install the parallel schedulers (dask and/or spark).

There is a completely different strategy you may want to consider for
early experimentation.   That is, if you have a initial, working prototype
that you need to evaluate you can consider the import-export approach.
That is, suppose you have one of those "dusty decks" from your advisor
that runs but only accepts SAC files as input and produces output as SAC files.
For initial experimenting a good initial solution for testing is often to
use MsPASS for data preparation, export your test data as SAC files, and then
run the prototype.   The format is not the point.  The point is that MsPASS
can be your friend in data preparation.

Adapting a Prototype
~~~~~~~~~~~~~~~~~~~~~~~~
We next consider the case of adapting an existing algorithm to be
used in MsPASS.  You can view this as the case where you know that "dusty deck"
FORTRAN code does what you want it to do, but you want to be able to run it
on a lot of data and the code just won't allow it.  For example, it
only works by reading a list of SAC files.

For this situation, you have three options:
-  If the "dusty deck" is written in C, you may be able to use pybind11,
   which we used in MsPASS to build the low-level functions needed for
   efficiency.  That approach, however, is "not for the faint of heart"
   to use a bad cliche.   pybind11 is a complicated package with
   incomplete documentation too common in open-source packages.
   It isn't horrible, but is often crytic and depends heavily on examples.
   If you use this model, you can use our binding code found in
   the mspass source code directory under `cxx/python` for guidance.
-  There are more packages out there than pybind11 to build python bindings for
   compiled languages.  If there is local expertise you can lean that
   have done python bindings, you may want to seek guidance from that person.
   A complete current list maintained by python.org an can be found
   `here<https://wiki.python.org/moin/IntegratingPythonWithOtherLanguages>`__.
-  If you are very familiar with python, you may find it easier to just
   translate the algorithm to python.   There are tools like Chat GPT that
   can help you, but if you are a student you would be advised to
   use our own brain as much as possible to do the translation so you fully
   understand what you produce.

In any case, if you are adapting an algorithm you will want to design
a thorough set of tests to make sure it does what you expect it to do.
How exhaustive your testing program needs to be is a judgement call based
on the time you have available and the level of complexity of the algorithm.
In MsPASS we use
`pytest<https://docs.pytest.org/en/latest/>`__
to validate the entire code-base whenever we change anything
and commit the result to github.  For most research,however, that is overkill, and
few are likely to invest the time in learning yet another package
(pytest has it's own quirks).  Nonetheless, it is essential you test
any prototype to make sure it does what you think it should do.
You risk professional suicide if you base a science paper on a computation
that is fundamentally flawed.   It is a personal judgement call about
what you need to test to have the confidence your result is correct.
The complexities of all possible decision defy any attempt we could
make to advise you.   As with all research, seek feedback from collaborators
and colleagues to help validate your results as best you can.

A final point to consider for algorithms written in python is that there
are packages to improve the performance of python code.  A couple simple
ones I know of are:

1.  You could consider using `cython<https://cython.org/>`__
    to effectively compile your python
    code into C.   It has it's own quirks, but may be a fast solution for
    first-order improvement of an algorithm.
2.  A more limited approach is to used `numba<https://numba.pydata.org/>`__
    to parallelize sections of a python function.   That is known to
    significantly improve speed of some pure-python functions, but requires
    installing yet another package and the result may not play well with
    dask or pyspark.  The reason is that numba uses threading that may
    collide with thread pools that are used by default in dask.   I do not
    really know if this is a problem, but it might be.  If you go down that
    route and read this, please pass on what you find to the MsPASS development
    team with a github issue.

When you have a tested function(s) that implement your idea, the
first thing I would recommend anyone do is try it out with a serial
workflow (i.e. a program that more-or-less boils down to a loop over
instances of the same data type) on a more limited data set.   That could start
with simulation data, but should eventually include some real data.
Even cleaned data from archives tend to have issues you won't encounter
with simulation data, unless the simulation was designed to test a known issue.
When you have a working serial job, parallelize it on your local system
and make sure you get the same answer you did with the serial job.
Only then would I recommend you port your prototype to a large cluster
for a full scale run.  At that point our work naturally falls into the next
section.

Tuning Processing to a Large Dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
