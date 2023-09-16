.. _development_stategies:

How do I develop a new workflow from scratch?
==================================================

The answer to this question depends on the complexity of the workflow you
need to develop.   We discuss this here in terms of two endmembers:  (1) a
workflow that can be reduced to a single python script with one or two simple
adapter functions, and (2) a complex program that requires adapting algorithms
from an external package. The two subsections of this page discuss these two
endmembers.  Most actual work will fall between these two endmembers and
may require a hybrid approach.

Simple workflows
~~~~~~~~~~~~~~~~~~~~

Simple workflows can usually be developed using only the standard MsPASS
container and the jupyter lab interface.  There are a number of approaches
discussed in common tutorials found by searching with the keywords
"jupyter debug" in this situation.

#. Inserting simple print statements to display the value of suspected variables.
#. Inserting matplotlib graphics to visualize data at an intermediate stage of
   processing.
#. If you have a box that throws a mysterious that is not self-explanatory
   the `%debug` magic command can be useful.   After the box with the
   exception insert a code box, add the magic command, and execute push
   the run button for the new cell.
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

- `This<https://notebook.community/tschinz/iPython_Workspace/00_Admin/Features/Jupyter%20Debug>`__
  concise but perhaps cryptic introduction for jupyter lab and pdb.
- `This<https://towardsdatascience.com/debugging-jupyter-notebooks-will-boost-your-productivity-a33387f4fa62>`__
  good overview that discusses a range of strategies for jupyter notebooks.

As always for a topic like this a google search will give you a long
string of variable quality resources.

Complex developments
~~~~~~~~~~~~~~~~~~~~~~~~~
Because MsPASS uses python as the job control language, developing
a workflow differs little from programming.   Advanced users may decide
they need to add features not in the framework that are too extensive
to implement as code blocks in the jupyter format.   Alternatively,
python is has a wide range of open-source toolkits that you may find the
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
    and `pycharm<https://www.jetbrains.com/pycharm/>`__, but there are
    a number of others.   IDEs dramatically improve most people's ability
    to test and debug python applications compared to a simple editor
    and pdb.   If you aren't already using one, choose one and use it.
#.  Install a local copy of MsPASS.  Unfortunately, there are currently
    no simple ways we know of to
    run any of these IDEs with an instance of mspass running via
    docker or singularity. For this reason you will likely find it necessary
    to install a local copy of mspass on your development desktop.
    The process for doing that is described in a wiki page on github
    found `here<https://github.com/mspass-team/mspass/wiki/Compiling-MsPASS-from-source-code>`__.
#.  The wiki page referenced above describes how to install dask and/or spark.
    We have found many projects do not require the parallel framework and
    can be reduced to writing and testing an appropriate set of functions
    and/or python classes that implement the extension you need.  Once a
    function to preform a task is written and thoroughly tested it can
    be plugged into the framework as just another python function used in
    a map or reduce operator.
#.  Design a local test of your application that can be tested in serial
    form on a small, test data set.  Move to a massive HPC or cloud system
    only when needed and you are confident your application is in as well
    tested as realistically possible
#.  Here we assume the tool you are developing can be placed in one or
    files that can serve as a standard python module; meaning something
    you can "import" with the right path to the file.
#.  To test a python function with the mspass container, copy your python
    code to a directory you mount with the appropriate docker or singularity run
    incantation.  When the container is running launch a terminal window with
    the jupyter interface and copy the python code to an appropriate
    directory in the mspass installation file tree in the container file
    system (currently `/opt/conda/lib/python3.10/site-packages/mspasspy`
    but not the `python3.10` could change as new python versions appear.)
    Then you can import your module as within the container using
    the standard import syntax with the top level, in this case,
    being defined as `mspasspy`.  To be more concrete if you have a new
    python module file called `myfancystuff.py` you copy into the `algorithms`
    directory you could expose a test script in a notebook to the new module with
    something like this:  `import mspasspy.algorithms.myfancystuff as mystuff`.
#.  Once you are finished testing you can do one of two things. (a) submit
    you code as a pull request to the github site for mspass.   If accepted it
    becomes part of mspass.  (b) build a custom docker container that
    adds your software as an extension of the mspass container.  The docker
    documentation and the examples in the top level directory for the MsPASS
    source code tree should get you started.  It is beyond the scope of this
    document to give details of that process. 
