.. _deploy_mspass_with_conda_and_coiled:

Deploy MsPASS with Conda and Coiled
===============================

Overview
-------------
This section provides a concise summary of the steps required to run 
MsPASS using Conda and Coiled. The instructions assume you are working 
in a cloud environment (AWS, GCP, Azure). You can also only use conda 
to install MsPASS locally without Coiled (step 3).

1. Install Coiled
---------------------
Fetch and install Coiled following instructions on the
`Coiled web site <https://docs.coiled.io/user_guide/setup/index.html>`__.

Install the Coiled client Python library with pip or conda.

.. code-block::

    pip install coiled "dask[complete]"
    coiled login

This will redirect you to the Coiled website to authenticate your computer. 


2. Connect to your cloud
---------------------------------------------
Next grant Coiled permission to run in your cloud account(AWS, GCP, Azure). 
Coiled creates the IAM policies and network configuration for your account, 
asking you for permission at each step. 

.. code-block::

    coiled setup aws
    coiled setup gcp

You can configure Coiled with custom network configuration in the 
`user portal <https://cloud.coiled.io/settings/setup/infrastructure>`__.

3. Get MsPASS Conda package
-------------------------------------------
If you have not run MsPASS before you will need to get the 
`conda package <https://anaconda.org/cxwang/mspasspy>`__.
from our standard repository.  Alternatively if you want to get the most
recent updates you may also need to do this step. 

We strongly advise you create a separate environment for mspass
to avoiding breaking any existing python packages you may have 
installed previous.  Make sure you are on the `base` environment 
ant enter

.. code-block::

    conda create --name mspass

Noting the name "mspass" is not special and it can be changed if you 
prefer something else.  You chould then make the new 
environment current with the standand conda command:

.. code-block::

    conda activate mspass

You will almost certainly need to add key "channels" as follows:

.. code-block::

    conda config --add channels cxwang 
    conda config --add channels conda-forge 

Then install mspass in this environment with

.. code-block::

    conda install -y mspasspy

4. Run MsPASS
-------------------------
After installing, Coiled will then pick up all those things installed locally, 
and install them on your cluster. For running things on Coiled, you could 
try `coiled run your_code.py` (or follow one of the examples in 
`coiled docs <https://docs.coiled.io/user_guide/usage/examples.html>`__)!


For example, to connect to MongoDB using Atlas:

.. code-block::

    from pymongo.mongo_client import MongoClient
    from pymongo.server_api import ServerApi
    from urllib.parse import quote_plus
    username = "your username"
    password = "your password"

    # URL-encode the username and password
    uri_username = quote_plus(username)
    uri_password = quote_plus(password)
    uri = "mongodb+srv://username:somestring@cluster0.domain.mongodb.net/"

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

If the ping success, it means we successfully connect to your remote mongo database.
Let's build a MsPASS client and start to use MsPASS:

.. code-block::
    
    from mspasspy.db.client import DBClient
    dbclient=DBClient(uri)
    dbclient.list_database_names() # view all the databases
    db = dbclient['mspass']        # choose a database
    db.index_mseed_file('CIGSC__BHZ___2017180.ms', some_path) # index mseed files


To use Dask:

.. code-block::

    from coiled import Cluster

    cluster = Cluster(n_workers=20)
    client = cluster.get_client()

Once you have a Dask cluster you can then run Python code on that cluster. 
Here is a simple code you could run:

.. code-block::

    def inc(x):
        return x + 1

    future = client.submit(inc, 10)
    future.result() # returns 11

You can find more useful examples in Coiled documentation and reach out to 
Coiled team (support@coiled.io) for any usage questions.
