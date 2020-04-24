.. _tutorial_concepts:

Key Concepts
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