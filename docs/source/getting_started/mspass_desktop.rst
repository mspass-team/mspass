.. _mspass_desktop:

Running MsPASS on a Desktop Computer
=====================================

Install MsPASS Desktop Application
------------------------------------
Install docker
~~~~~~~~~~~~~~~~
The desktop application to launch MsPASS requires :code:`docker desktop`.  If
docker is not already installed on your desktop machine you will need to do so.
Docker is free for non-commercial users.
Installers for all standard desktop operating systems can be found on the
`docker web site <https://docs.docker.com/get-docker/>`__.

Launch docker
~~~~~~~~~~~~~~~
Documentation for operating docker for each platform can be found
on hyperlinks from the download page.   Note on linux docker
implementation is normally set up as a daemon and managed with command line tools.
After an initial install on linux you may need to reboot your desktop or
manually start the daemon.
MacOS and Windows use a graphical user interface that you may need to
launch manually by the usual double-click or start procedure for
applications.  See the docker documentation for guidance on your platform.

Get the MsPASS container
~~~~~~~~~~~~~~~~~~~~~~~~~
Use the command line interface to "pull" the MsPASS container using
the following incantation:

.. code-block::

    docker pull mspass/mspass

Verify python version
~~~~~~~~~~~~~~~~~~~~~~
The MsPASS desktop application requires a version of python of
3.10 or above.  Verify what you have running as follow:

.. code-block::

  python --version

If necessary, upgrade your version of python to 3.10 or above.   Note if you
use a python virtual environment management package like conda or pyenv, which we would
advise, you may want to create a :code:`mspass` environment that uses an acceptable version
of python and runs the rest of this procedure from that environment.

Install mspass-desktop
~~~~~~~~~~~~~~~~~~~~~~~~
If you are using an pyenv or conda make sure the appropriate environment is
active.  You can then install the mspass desktop application with pip
as follows:

.. code-block::

  pip install mspass_launcher

If you aren't using anaconda or pyenv with a special virtual environment, we suggest you use this variant:

.. code-block::

  pip install --user mspass_launcher

That will install the package under :code:`~/.local`, which is usually appropriate for a single user desktop.


Running mspass-desktop
------------------------
Use the terminal application on your platform and navigate
(i.e. use the cd command) to the working directory for your data set.

.. note::
  Currently there is no control where files written by mspass are written.
  All are relative to the working directory from which you launch
  mspass-desktop.   The application will create a `data` directory
  which contains the database files, `logs` which contains log files
  written by the different services run under docker compose, and a `work`
  directory used as scratch space by dask.  It also creates an empty
  `scoped` directory that is not used for this application of the
  MsPASS framework.

  Use the command line tool to launch the graphical user interface as follows:

  .. code-block::

    mspass-desktop

  That should bring up the following window:

  .. _desktop_startup_window:

.. figure:: ../_static/figures/desktop_startup_window.tif
    :width: 600px
    :align: center

    Initial window created by running mspass-desktop.   Note only the
    launch button is active and all the status widgets are red to
    indicate that service is down.

Push the "Launch" button and the display should change to something similar
to the following:

.. _desktop_operating_window:

.. figure:: ../_static/figures/desktop_operating_window.tif
  :width: 600px
  :align: center

  MsPASS desktop window with all services running.   Note the status
  widgets show green when that service is running.  If any turn red
  during operation the system will be be functional.

For interactive use with jupyter lab, push the "Jupyter" button.   It
should launch a browser tab with jupyter lab running.

Note that after pushing the "Jupyter" button the center frame of the GUI
with the label "Jupyter URL" should contain the text of the URL that
defines a connection to the jupter server.   If the jupyter auto launch fails you can
copy that url and paste it into a browser window to obtain a jupyter
lab window for running mspass.

If you have an existing python script you want to just run, push the
"Run" button on the MsPASS desktop GUI.  That will create a new window
that should look like the following:

.. _desktop_run_window:

.. figure:: ../static_figures/desktop_run_window.tif

   Window launched by run button.

Enter the name of your script in the box with the label
"File name of python file to be run" and push the "Run it" button.
Any output of that script will appear in the terminal window from which
you launched mspass-desktop.   If you need to capture the output
consider using a tool like the unix "script" command.

Potential Issues
------------------
GUI launches but is incomplete
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
There is a known problem with :code:`mspass-desktop` running on MacOS
created by a classic python package collision with the
:code:`tkinter` module used to drive the :code:`mspass-desktop` GUI.
If you are using an Apple computer and have an issue consult
`this issue page<https://github.com/mspass-team/mspass_launcher/issues/7>`_
on GitHub for possible solutions.

Need to Edit Configuration Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
In most cases the default configuration files will not need to be changed.
If you encounter problems with the launching or running :code:`mspass-desktop`,
you may want to look through the configuration files and see if
editing the files could provide a solution,

If you need to alter the master configuration files, we recommend you
first copy the master files somewhere else in case you need to restore the
originals.
By default, the application will look for a directory
:code:`./data/yaml` where "." is the location where the python library
code for :code:`mspass_launcher` was installed by pip.  After making a
copy the simplest approach is to edit the files there directly.

We can anticipate the following that might prove necessary:

1.  The :code:`DesktopCluster.yaml` file is read directly by
    :code:`docker compose` to launch the local cluster service needed to
    run MsPASS.   If something goes wrong launching when
    :code:`docker compose`  launches the containers, which hopefully will
    be clear from error messages, you may need to edit this file.
    Consult the `docker compose documentation<https://docs.docker.com/compose/>`_ for guidance.
2.  The :code:`MsPASSDesktopGUI.yaml` file has a few parameters you may want
    to customize:

    -  The *web_browser* attribute is "firefox" by default.   If you want to
       use a different browser enter a different name after the
       *web_browser* keyword.  "Safari" (Note capital S) is known
       to work on MacOS, but anything else will be an adventure.  Note a key
       thing is the name needs to resolvable by the command line interface.
       Note, especially, that for a Mac that is not just the browser name.
       e.g. on a Mac you would open firefox from a terminal this way:
       :code:`open -a firefox`.   The Windows interface is currently
       adventure land.
    -  The two attributes *minimum_window_size_x* and *minimum_window_size_y*
       set the base GUI size in pixels.   If you have an unusually high
       resolution or low resolution screen, you may want to change these
       attributes.  The most likely service to cause a problem is the
       MongoDB container which is known to take a few seconds to launch
       on most desktops.
    -  The default for *engine_startup_delay_time* is fairly conservative an
       causes a delay you will notice in launching the GUI.   If you have a
       fast machine that is usually lightly loaded you could try reducing the
       default if you are impatient.   If the launching process throws a python
       error exception hinting at a startup issue, consider increasing this
       parameter.
    -  The default for *status_monitor_time_interval* is 10 s.  That parameter
       defines how frequently the services launched by :code: `docker compose`
       are checked for state-of-health.   That checking does not seem to be a
       heavy load so reducing that interval might be helpful if you are having
       a problem with a crashing container.
