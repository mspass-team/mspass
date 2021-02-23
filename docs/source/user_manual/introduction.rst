Introduction
=================================
MsPASS Features
~~~~~~~~~~~~~~~~
MsPASS is an acronymn that comes from Massively Parallel Analysis System for Seismologists.
We begin with some key features of MsPASS.

-   As the name suggests MsPASS is a domain-specific package for seismologists.
    We emphasize, however, that our definition of seismologists is
    broad.  It includes exploration geophysicists and anyone in any field that
    needs to work with data that match the data models described in this
    manual.
-   Few seismologists have strong expertise in modern information technology.
    Furthermore, installing a software package can prove challenging today
    on a desktop on which you may have special privileges.  On large hypocenter
    systems installation is usually impossible without a long string of
    meetings and correspondence with system managers.  For this reason MsPASS uses
    modern container technology to simplify installation.  That allows you
    as the user to install MsPASS without system privileges.  Containers are
    also essential for operating the package in a cloud system.
-   Another keyword in the acronymn is "parallel".  Computer technology reached
    the physical limit of single CPU computers many years ago.  NO existing
    package for handling earthquake data provides consistent support for
    parallel processing.
-   A first order goal of MsPASS was building
    a consistent framework to allow scalability from a single desktop
    machine with multiple cores to a giant cloud-based system. In MsPASS
    we developed a simplfied API for running processing in parallel.
    The API makes it relatively easy to prototype a workflow with a test data set before
    porting it to a large cluster to handle a more massive processing job.
-   MsPASS uses an integrated database management system that
    was known previously to perform well in a massively parallel environment.
    We use a stable, open-source system called MongoDB.  Our database api
    aims to abstract interactions with MongoDB as much as possible, but
    some knowledge of the query language used by MongoDB will be required to
    use the package effectively.  There are several published books and
    extensive documentation and tutorials are available online.  Web
    searches for MongoDB commands or a book at your side are an essential tool for
    working with MsPASS at any level.
-   MsPASS uses python as the job control language.  This is in contrast to
    traditional programs like SAC or older seismic processing systems that
    use a custom command interpreter.   We assert that custom languages
    like that in SAC or even the unix shell will become the equivalent of
    Latin as a language in the coming years.  Python has emerged as the
    most common glue language use within software packages.  It was thus
    the clear choice as the driver language for MsPASS.
-   Although python has huge advantages as "glue language" and as a way to
    quickly develop prototypes, it also has a serious limitation.   Python,
    like matlab's command line language, is an interpreted language.  That means
    it is essentially compiled on the fly.  Some classes of algorithms
    will run orders of magnitude faster if implemented in a compiled language
    like C/C++ or FORTRAN compared to python.  For this reason, we implemented
    core data objects in C++.  Any python package with any hope of
    performance follows this same model.  `Obspy<https://docs.obspy.org/>`__
    does this by manipulating
    sample data with numpy and some core C libraries.
    `Antelope<https://www.brtt.com>`__ python
    does this as well by implementing core functions in C. We follow this
    model.  Python functions and classes implemented in C/C++ the MsPASS
    module hierarchy under mspasspy.ccore.
-   Errors are a universal issue in large scale data processing.   As the
    size of a data set increases it is becomes a increasingly difficult to
    guarantee all the data are "clean".  By that, we mean one or more
    algorithms may treat "the problem" as an unrecoverable error.  Handling
    errors in a large scale processing environment is problematic for a long
    list of reasons.  We solve this in MsPASS by having error logs be an
    intrisic part of the data.   In addition,
    following a well established approach
    used is seismic reflection systems since the 1960s MsPASS provides a
    integrated "kill" mechanism that allows data to be carried along
    but ignored.  That model maps well to massively parallel scheduling
    because dead data are treated like live data but they just process faster.
    The error messages posted to any data are automatically saved in the
    database when the data object is saved.
-   To promote reproducible science MsPASS has an integrated processing history
    capability.  The goal of that component of MsPASS is to ultimately allow
    publication of the processing workflow used in a scientific paper that
    would allow the reader to reproduce the data that paper used.  At the
    time this user's manuals was written that part of the system is incomplete.
    For that reason (and for efficiency) MsPASS processing functions make
    handling history optional and by default it is turned off.  If the system
    grows as we hope that limitation may disappear.
-   The design of MsPASS has stressed leading edge but not bleeding edge open-source
    technologies.  MsPASS was assembled from
    a long list of general purpose, open-source packages.
    Some are C/C++ libraries that
    are linked with code in the ccore modules (e.g. `boost<https://www.boost.org/>`__
    and the `GNU Scientific library<https://www.gnu.org/software/gsl/>`__)
    and some are python packages like obspy.

Getting Started
~~~~~~~~~~~~~~~~~~~

The first step to use MsPASS is to install a local copy that you can use
for initial experimentation.
`Click here <https://github.com/wangyinz/mspass/wiki/Using-MsPASS-with-Docker>`__
for installation instructions.  (CHANGE ME - POINTS NOW TO WIKI PAGE BUT
WE NEED A REAL INSTALLATION PAGE)

We have an extensive set of tutorials based on jupyter notebooks
found `here<>`___ (DEAD LINK - ADD when we have a final location for tutorials).
For most people these are a good way to learn the package on their own.

Organization of User Manual
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The titles of the sections of this manual should serve as guides for
how to learn more about a topic of interest.   Except for this section the
manual is not intended to be read in the order of the topics posted as
hypertext in the contents page.  A learning model most people find most
effective is to work through the tutorials and reading sections of this
manual as questions arise or by following hypertext links from the tutorials.
