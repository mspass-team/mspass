.. _development_strategies:

How do I develop a new workflow from scratch?
==================================================
*Answer by Prof. Gary L. Pavlis*

The answer to this question depends on the complexity of the workflow you
need to develop.   We discuss this here in terms of two endmembers:  (1) a
workflow that can be reduced to a single python script with one or two simple
adapter functions, and (2) a complex program that requires adapting algorithms
from an external package. This page is designed in a progression from these
endmembers.

For all cases there it is important to first recognize a fundamental
starting point, at least in present IT environment I suspect most
seismologists work in.   That is, you should expect:

#.  You absolutely will need an interactive framework to develop any
    workflow.  For most people, that means a local desktop or laptop
    with docker installed.  It is possible to use only a web browser
    and with connections to a cluster, but that is guaranteed to be
    a lot more difficult to do.  The fact that containerization makes
    the transition from a desktop a cluster much simpler makes the
    model of using a desktop for initial development theh only sensible
    norm.
#.  Every single example I have ever created to run MsPASS was
    best created by first writing the workflow as a serial process
    with a single outer loop over the dataset elements.  Using
    guidelines in :ref:`parallel_processing` convert to a parallel
    equivalent only after you get the syntax and variables all defined
    with the serial job.
#.  Most  workflows reduce to one or more sections that have the
    generic structure:  read data -> process data -> save result.
    To debug a parallel workflow on your desktop use a subset of
    your data copied to your workstation and run the test on
    the smaller subset that defines the "read data" section.
    That approach provides a simple way to validate the workflow has
    no obvious errors like python syntax errors and usage errors.
    It is also a very good idea to use your desktop's system monitor
    to watch the workflow's cpu and memory usage while running the
    test data set.   You may need to tune the memory use of the workflow
    based on concepts described in :ref:`memory_management` section
    of this manual.
#.  If you are running on a (now ancient) computer with only on core
    you will not be able to test the parallel version on your desktop/laptop.
    If your machine has only two cores, keep your test data set to
    the minimum possible because you may bring the machine to its knees
    while you run the test. In general, my advice would be that to a
    desktop/laptop parallel test configure docker to only use two cores.
    I have found it is easy to bring a desktop/laptop to it's knees if
    you use all or a large fraction of the available cores.  Hence, the
    corollary to keep in mind is you shouldn't expect to do much else
    like web surfing or email
    on your desktop/laptop while running your test data set. You can expect
    sluggish performance if parallelization is working as it should.

Simple workflows
~~~~~~~~~~~~~~~~~~~~

Simple workflows can usually be developed using only the standard MsPASS
container and the jupyter lab interface.  There are a number of approaches
discussed in common tutorials found by searching with the keywords
"jupyter debug" in this situation.

#. Inserting simple print statements to display the value of suspected variables.
#. Inserting matplotlib graphics to visualize data at an intermediate stage of
   processing.
#. If you have a box that throws a mysterious exception that is not self-explanatory
   the `%debug` magic command can be useful.
   To use this feature add a new code box after the cell with problems, put
   that command in the box, and push the jupyter run button.  You will get
   an ipython prompt you can use to investigate variables defined where the
   error occurred.
#. Use the jupyter lab debugger.
#. Enable the pdb debugger

In addition to the technical approaches for code debugging it is worth
pointing out a completely different point;  use the jupyter notebook
structure to document your work.   Use `Markdown` boxes to at least provide
notes to yourself about what you are trying to do with the code and
any key background.   In addition, as always use comments within the code
to clarify any sections using some less than obvious trick or dependent
upon some obscure features that you exploited in writing that algorithm.

For simple workflows the recently developed debugger for the jupyter lab
will usually be sufficient.  If an error occurs outside your notebook,
however, the best strategy at present is to use pdb, which is included
in the standard mspass container.   There are numerous
tutorials on using pdb to debug a jupyter notebook.  Here are a
couple we found useful:

- `This <https://notebook.community/tschinz/iPython_Workspace/00_Admin/Features/Jupyter%20Debug>`__
  concise but perhaps cryptic introduction for jupyter lab and pdb.
- `This <https://towardsdatascience.com/debugging-jupyter-notebooks-will-boost-your-productivity-a33387f4fa62>`__
  good overview that discusses a range of strategies for jupyter notebooks.

As always for a topic like this a google search will give you a long
string of variable quality resources.

Complex developments
~~~~~~~~~~~~~~~~~~~~~~~~~
Because MsPASS uses python as the job control language, developing
a workflow differs little from programming.   Advanced users may decide
they need to add features not in the framework that are too extensive
to implement as code blocks in the jupyter format.   Alternatively,
python has a wide range of open-source toolkits that you may find the
need to add to facilitate your custom research code.   Here we provide
suggestions on basic strategies.

The first point is that advanced development is best done on a desktop
where interactive access is the norm.   Jumping immediately to an HPC
cluster is destined to create frustration.  Some recommended steps to
create a development environment for MsPASS on a desktop are the
following:

#.  Install an IDE of your choice.
    If you are doing extensive python development you are likely already
    familiar with one of the standard Integrated Development Environments
    like `pycharm <https://www.jetbrains.com/pycharm/>`__ or
    `spyder <https://www.spyder-ide.org/>`__, but there are
    a number of others.   IDEs dramatically improve most people's ability
    to test and debug python applications compared to a simple editor
    and pdb.   If you aren't already using one, choose one and use it.
#.  Install a local copy of MsPASS.  Unfortunately, there are currently
    no simple ways we know of to
    run any of these IDEs with an instance of mspass running via
    docker or singularity. For this reason you will likely find it necessary
    to install a local copy of mspass on your development desktop.
    The process for doing that is described in a wiki page on github
    found `here <https://github.com/mspass-team/mspass/wiki/Compiling-MsPASS-from-source-code>`__.
#.  The wiki page referenced above describes how to install dask and/or spark.
    We have found many projects do not require the parallel framework and
    can be reduced to writing and testing an appropriate set of functions
    and/or python classes that implement the extension you need.  Once a
    function to preform a task is written and thoroughly tested it can
    be plugged into the framework as just another python function used in
    a map or reduce operator.
#.  Design a local test of your application that can be tested in serial
    form on a small, test data set.  Move to a massive HPC or cloud system
    only when needed and you are confident your application is as well
    tested as realistically possible
#.  Here we assume the tool you are developing can be placed in one or
    more files that can serve as a standard python module; meaning something
    you can "import" with the right path to the file.  If you don't know how
    to build a python module file there are huge numbers of internet
    resources easily found with a web search.
#.  To test a python function with the mspass container, copy your python
    code to a directory you mount with the appropriate docker or singularity run
    incantation.  The simplest way to do that is to just put your python
    script in the same directory as your notebook.   In that case, the
    notebook code need only include a simple `import`.   e.g. if you have
    your code saved in a file `mymodule.py` and you want to use a function
    in that module called `myfunction`, in your notebook you would just
    enter this simple, failry standard command:

    .. code-block:: python

      from mymodule import myfunction

    If `mymodule` is located in a different directory use the
    docker "--mount" option or apptainer/singularity "-B" options to
    "bind" that directory to the container.   For example, suppose we have
    module `mymodule.py` stored in a directory called `/home/myname/python`.
    With docker this could be mounted on the standard container
    with the following incantation:

    .. code-block:: bash

      docker run --mount src=/home/myname/python,target=/mnt,type=bind -p 8888:8888 mspass/mspass

    To make that module accessible with the same import command as above you
    would need to change the python search path.  For this example, you could
    use this incanation:

    .. code-block:: python

      import sys
      sys.path.append('/mnt')

#.  Once you are finished testing you can do one of two things to make
    it a more durable feature. (a) Assimilante
    your module into mspass and submit
    you code as a pull request to the github site for mspass.   If accepted it
    becomes part of mspass.  (b) Build a custom docker container that
    adds your software as an extension of the mspass container.  The docker
    documentation and the examples in the top level directory for the MsPASS
    source code tree should get you started.  It is beyond the scope of this
    document to give details of that process.
