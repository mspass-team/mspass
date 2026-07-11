.. _development_strategies:

How do I develop a new workflow from scratch?
==================================================
*Answer by Prof. Gary L. Pavlis*

The answer to this question depends on the complexity of the workflow you
need to develop.   We discuss this here in terms of two endmembers:  (1) a
workflow that can be reduced to a single Python script with one or two simple
adapter functions, and (2) a complex program that requires adapting algorithms
from an external package. This page is designed in a progression from these
endmembers.

For all cases it is important to recognize a fundamental starting point:

#.  You absolutely will need an interactive framework to develop any
    workflow.  For most people, that means a local desktop or laptop
    with Docker installed, or a local Conda environment.  It is possible to use only a web browser
    and with connections to a cluster, but that is guaranteed to be
    a lot more difficult to do.  The fact that containerization makes
    the transition from a desktop to a cluster much simpler makes the
    model of using a desktop for initial development the most practical norm.
#.  Every single example I have ever created to run MsPASS was
    best created by first writing the workflow as a serial process
    with a single outer loop over the dataset elements.  Using
    guidelines in :ref:`parallel_processing`, convert to a parallel
    equivalent only after you get the syntax and variables all defined
    with the serial job.
#.  Most  workflows reduce to one or more sections that have the
    generic structure:  read data -> process data -> save result.
    To debug a parallel workflow on your desktop use a subset of
    your data copied to your workstation and run the test on
    the smaller subset that defines the "read data" section.
    That approach provides a simple way to validate the workflow has
    no obvious errors like Python syntax errors and usage errors.
    It is also a very good idea to use your desktop's system monitor
    to watch the workflow's cpu and memory usage while running the
    test data set.   You may need to tune the memory use of the workflow
    based on concepts described in :ref:`memory_management` section
    of this manual.
#.  If you are running on a computer with only one core
    you will not be able to test the parallel version on your desktop/laptop.
    If your machine has only two cores, keep your test data set to
    the minimum possible because you may bring the machine to its knees
    while you run the test.  In general, for a desktop or laptop parallel
    test, configure Docker to use only two cores.
    I have found it is easy to bring a desktop/laptop to its knees if
    you use all or a large fraction of the available cores.  Hence, the
    corollary to keep in mind is you shouldn't expect to do much else
    like web surfing or email
    on your desktop/laptop while running your test data set. You can expect
    sluggish performance if parallelization is working as it should.

Simple workflows
~~~~~~~~~~~~~~~~~~~~

Simple workflows can usually be developed using only the standard MsPASS
container and the JupyterLab interface.  Useful approaches include:

#. Inserting simple print statements to display the value of suspected variables.
#. Inserting matplotlib graphics to visualize data at an intermediate stage of
   processing.
#. If you have a cell that throws a mysterious exception that is not self-explanatory,
   the ``%debug`` magic command can be useful.
   To use this feature, add a new code cell after the cell with problems, put
   that command in the cell, and run it.  You will get
   an IPython prompt you can use to investigate variables defined where the
   error occurred.
#. Use the `JupyterLab debugger
   <https://jupyterlab.readthedocs.io/en/stable/user/debugger.html>`__.
#. Enable Python's `pdb debugger
   <https://docs.python.org/3/library/pdb.html>`__.

In addition to the technical approaches for code debugging it is worth
pointing out a completely different point: use the Jupyter notebook
structure to document your work.  Use Markdown cells to at least provide
notes to yourself about what you are trying to do with the code and
any key background.   In addition, as always use comments within the code
to clarify any sections using some less than obvious trick or dependent
upon some obscure features that you exploited in writing that algorithm.

For simple workflows the JupyterLab debugger will usually be sufficient.
If an error occurs outside your notebook, use ``pdb``, which is included in
Python and in the standard MsPASS container.  The official links above cover
breakpoints, stack inspection, and variable inspection for both approaches.

Complex developments
~~~~~~~~~~~~~~~~~~~~~~~~~
Because MsPASS uses Python as the job control language, developing
a workflow differs little from programming.   Advanced users may decide
they need to add features not in the framework that are too extensive
to implement as code blocks in a Jupyter notebook.  Alternatively,
Python has a wide range of open-source toolkits that you may find the
need to add to facilitate your custom research code.   Here we provide
suggestions on basic strategies.

The first point is that advanced development is best done on a desktop
where interactive access is the norm.   Jumping immediately to an HPC
cluster is destined to create frustration.  Some recommended steps to
create a development environment for MsPASS on a desktop are the
following:

#.  Install an IDE of your choice.
    If you are doing extensive Python development you are likely already
    familiar with one of the standard Integrated Development Environments
    like `pycharm <https://www.jetbrains.com/pycharm/>`__ or
    `spyder <https://www.spyder-ide.org/>`__, but there are
    a number of others.   IDEs dramatically improve most people's ability
    to test and debug Python applications compared to a simple editor
    and pdb.   If you aren't already using one, choose one and use it.
#.  Install MsPASS in an environment your IDE can select as its Python
    interpreter.  For most workflow development, use the packaged
    :ref:`Conda installation <deploy_mspass_with_conda>`.  Contributors who
    need to modify MsPASS itself should follow the
    :ref:`source-build guide <advanced_setup_considerations>`.  Container-aware
    IDEs are another option, but their setup is specific to the IDE.
#.  Many projects do not require a distributed scheduler during algorithm
    development and
    can be reduced to writing and testing an appropriate set of functions
    and/or Python classes that implement the extension you need.  Once a
    function to perform a task is written and thoroughly tested it can
    be plugged into the framework as just another Python function used in
    a map or reduce operator.
#.  Design a local test of your application that can be tested in serial
    form on a small, test data set.  Move to a massive HPC or cloud system
    only when needed and you are confident your application is as well
    tested as realistically possible.
#.  Here we assume the tool you are developing can be placed in one or
    more files that can serve as a standard Python module, meaning something
    you can import when its directory is on the Python search path.  If you
    don't know how to build a Python module file there are many
    resources easily found with a web search.
#.  To test a Python function with the MsPASS container, copy your Python
    code to a directory you mount with the appropriate Docker or Apptainer run
    option.  The simplest way to do that is to put your Python
    script in the same directory as your notebook.   In that case, the
    notebook code need only include a simple import.  For example, if you have
    your code saved in ``mymodule.py`` and want to use ``myfunction`` from it:

    .. code-block:: python

      from mymodule import myfunction

    If ``mymodule.py`` is located in a different directory, use Docker's
    ``--mount`` option or Apptainer's ``--bind`` option to
    "bind" that directory to the container.   For example, suppose we have
    module ``mymodule.py`` stored in ``/home/myname/python``.
    With Docker this could be mounted on the standard container:

    .. code-block:: bash

      docker run --mount type=bind,source=/home/myname/python,target=/mnt -p 8888:8888 mspass/mspass

    To make that module accessible with the same import command as above you
    would need to change the python search path.  For this example, you could
    add that directory to the Python search path before importing:

    .. code-block:: python

      import sys
      sys.path.insert(0, "/mnt")

#.  Once you are finished testing you can do one of two things to make
    it a more durable feature: (a) integrate your module into MsPASS and submit
    your code as a pull request to the MsPASS GitHub repository, or (b) build
    a custom container image that adds your software as an extension of the
    MsPASS image.  The Docker documentation and examples in the top-level
    directory of the MsPASS source code tree should get you started.  It is
    beyond the scope of this
    document to give details of that process.
