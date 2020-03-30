Tutorial
========
This tutorial is currently built for the 2020 GAGE-SAGE Workshop. Please make
sure to go through the :ref:`Prerequisites <tutorial_prerequisites>` section
before attending the short course. We will turn it into a general tutorial of 
working with MsPASS while the development goes.


Key concepts
------------
-  **Docker** is a piece of software to create containers (lightweight
   virtual machine) that will run on your machine. It is best suited for
   a single host with multiple processors that can be exploited for
   parallel processing. See the related section on singularity for
   clusters.
-  A *container* in docker is a lightweight instance of a virtual
   machine that share a common configuration. The containers run largely in 
   isolation from each other.
-  **docker-compose** is a related tool for working with multiple docker
   containers that MsPASS uses for parallel operations. docker-compose
   is configured with a YAML file. For MsPASS an example configuration
   is stored in the top of the GitHub tree as the file docker-compose.yml.
-  An key feature of Docker is that it provides a way to standardize the
   setup of MongoDB and Spark, which would otherwise be a burden on most users.
-  It can be confusing to understand where data is stored in a virtual
   machine environment. In the discussion below files or data we
   reference that reside on a virtual machine will be set in italics.
   Local files/data will be referred to with normal font text.


.. _tutorial_prerequisites:

Prerequisites
-------------
There are two distinctly different steps needed to set up your system for 
MsPASS with Docker: (1) installing Docker and docker-compose, and (2) 
configuration of the virtual machine environment for running mspass. 


Setting up docker
~~~~~~~~~~~~~~~~~
To install Docker on machines that you have root access, please refer to
the guide `here <https://docs.docker.com/v17.12/docker-for-mac/install/>`__. 

On Macs, Docker is a normal package available for download
`here <https://www.docker.com/>`__. It is installed in the standard
manner most Mac user's will be familiar with. It is installed in the
Applications folder. To launch the docker daemon simply double click the
application icon in the usual Mac way. It will create an icon on the
task bar that can be used to terminate the daemon and a few other tasks
that are described in the documentation for the software.

For Linux system Docker would normally be installed through the package
manager used by that flavor of unix. For example,
`here <https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-18-04>`__
is a useful source for Ubuntu.

For linux systems we note two issues you may encounter that will speed
this process:

1. Without some tricks Docker can only be run with a sudo command. That
   means each ``docker`` call below would need to be change to ``sudo
   docker``. You can manipulate groups to get your user name in the same 
   group as Docker to avoid that. There are variants in Unix about how groups are handled.
   `Follow this link <https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-18-04>`__ 
   for instructions on Ubuntu. You also may find it necessary to restart 
   your machine to get the revised groups to be recognized.
2. You will need both docker and docker-compose. Unix package managers
   may split them. e.g. on Ubuntu you need to use ``apt-get`` for both the
   key ``docker`` and ``docker-compose``.

To proceed from here we assume Docker has been installed and the Docker
daemon is running in the background.

Downloading the MsPASS Container Image
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once you have docker setup properly, use the following command in a
terminal to pull the MsPASS image from Docker Hub to your local machine:

::

   docker pull wangyinz/mspass

Be patient as this can take a few minutes. Note you can run this command
from anywhere. It loads data only in the virtual machine data space so
you will not see anything happen in the directory where you run this
command but it will eat off a few hundred megabytes on your disk. Any
data created and stored inside the container will be opaque from the
local system (outside) except for the ones in the directories that are
mounted to the container.
