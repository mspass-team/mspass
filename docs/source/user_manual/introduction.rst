.. _user_manual_introduction:

Introduction
=================================

MsPASS Features
~~~~~~~~~~~~~~~~

MsPASS is an acronymn that stands for Massively Parallel Analysis System for Seismologists.
Some key features of MsPASS are the following:

-   As the name suggests MsPASS is a domain-specific package for seismologists.
    We emphasize, however, that our definition of seismologists is
    broad.  It includes exploration geophysicists and anyone in any field that
    needs to work with data that match the data models described in this
    manual.

-   Few seismologists have strong expertise in modern information technology.
    Furthermore, installing a software package can prove challenging today
    even on a desktop on which you may have special privileges.  On large high performance (HPC)
    systems installation is usually impossible to install special software
    without a long string of
    meetings and correspondence with system managers.  For this reason MsPASS uses
    modern container technology to simplify installation.  That allows you
    as the user to install MsPASS without system privileges.  Containers are
    also essential for operating the package in a cloud system.

-   Another keyword in the acronym is "parallel".  Computer technology reached
    the physical limit of single CPU computers many years ago.  NO existing
    open-source package for handling earthquake data provided consistent support for
    parallel processing until MsPASS.

-   A first order goal of MsPASS was building
    a consistent framework to allow scalability from a single desktop
    machine with multiple cores to a giant cloud-based system. In MsPASS
    we developed a simplified API for running processing in parallel.
    The API makes it relatively easy to prototype a workflow with a test data set before
    porting it to a large cluster to handle a more massive processing job.

-   MsPASS uses an integrated database management system that
    was known previously to perform well in a massively parallel environment.
    We use a stable, open-source system called MongoDB.  Our database API
    aims to abstract interactions with MongoDB as much as possible, but
    some knowledge of the query language used by MongoDB will be required to
    use the package effectively.  There are several published books on MongoDB and
    extensive documentation and tutorials are available online for the package.  Web
    searches for MongoDB commands or a book at your side are an essential tool for
    working with MsPASS at any level.

-   MsPASS uses python as the job control language.  This is in contrast to
    traditional programs like SAC or older seismic processing systems that
    use a custom command interpreter.   We assert that custom languages
    like that in SAC or even the unix shell will become the equivalent of
    Latin as a language in the coming years.  Python has emerged as the
    most common glue language used within open-source software packages.  It was thus
    the clear choice as the driver language for MsPASS.

-   Although python has huge advantages as "glue language" and as a way to
    quickly develop prototypes, it also has a serious limitation.   Python,
    like matlab's command line language, is an interpreted language.  That means
    it is essentially compiled on the fly.  Some classes of algorithms
    will run orders of magnitude faster if implemented in a compiled language
    like C/C++ or FORTRAN compared to python.  For this reason, we implemented
    core data objects in C++.  Any python package with any hope of
    performance follows this same model.  `Obspy <https://docs.obspy.org/>`__
    does this by manipulating
    sample data with numpy and some core C libraries.
    `Antelope <https://www.brtt.com>`__ python
    does this as well by implementing core functions in C. We follow this
    model. Python functions and classes implemented in C/C++ are all
    found in the MsPASS hierarchy as all modules under mspasspy.ccore.

-   Errors are a universal issue in large-scale data processing.   As the
    size of a data set increases it is becomes an increasingly difficult to
    guarantee all the data are "clean".  By that, we mean one or more
    algorithms may treat a particular data problem as an unrecoverable error.  Handling
    errors in a large scale processing environment is problematic for a long
    list of reasons.  We solve this problem in MsPASS by having error logs be an
    intrinsic part of the data.   That approach is actually essential to
    preserve errors using Spark and DASK.  It would be very difficult to
    sort out problems from verbose log files that would otherwise be saved and
    interleaved in a scratch, logs directory if we used simpler print statements.
    Instead in MsPASS the error messages posted to any data object are automatically saved in the
    database when the data object is saved with cross-references to the data with
    which that error was associated.  In addition,
    following a well-established approach
    used is seismic reflection systems since the 1960s, MsPASS provides a
    integrated "kill" mechanism that allows data to be carried along
    but ignored.  That model maps well to massively parallel scheduling
    because dead data are treated like live data but they just process faster.

-   To promote reproducible science MsPASS has an integrated processing history
    capability.  The goal of that component of MsPASS is to ultimately allow
    publication of the processing workflow used in a scientific paper that
    would allow the reader to reproduce the data that paper used.  At the
    time this user's manuals was written that part of the system is incomplete.
    For that reason (and for efficiency) MsPASS processing functions make
    handling history optional and by default it is turned off.  If the system
    grows as we hope that limitation will disappear.

-   The design of MsPASS has stressed leading edge but not bleeding edge open-source
    technologies.  MsPASS was assembled from
    a long list of general purpose, open-source packages.
    Some are C/C++ libraries that
    are linked with code in the ccore modules (e.g. `boost <https://www.boost.org/>`__
    and the `GNU Scientific Library <https://www.gnu.org/software/gsl/>`__)
    and some are python packages like ObsPy.

Getting Started
~~~~~~~~~~~~~~~~~~~

The first step to use MsPASS is to install a local copy that you can use
for initial experimentation.
:ref:`Click here <run_mspass_with_docker>` for installation instructions.

We have an extensive set of tutorials based on jupyter notebooks
found `here <https://github.com/mspass-team/mspass_tutorial>`__.
For most people these are a good way to learn the package on their own.

Organization of User Manual
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The titles of the sections of this manual should serve as guides for
how to learn more about a topic of interest.   Except for this section the
manual is not intended to be read in the order of the topics posted as
hypertext in the contents page.  A learning model most people find most
effective is to work through the tutorials and reading sections of this
manual as questions arise or by following hypertext links from the tutorials.
