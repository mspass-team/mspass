#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prototype worker plugin for using dask on AWS to allow workers to
not have to instantiate an s3 client on each submit.   Modeled after
the mongodb worker plugin.

Specific to the supplied EarthScope/GeoLab workflow. Live deployment validation
is still required; the tests exercise credential lifecycle without AWS access.

Created on Mon Mar 23 09:26:00 2026

@author: pavlis
"""

import boto3
from contextlib import ExitStack
from botocore.config import Config
from botocore import UNSIGNED
from functools import lru_cache
from earthscope_sdk import EarthScopeClient
from dask.distributed import WorkerPlugin, get_worker

# this decorator is necessary to avoid the cost of instantiating a 
# client on serial tasks every time it is called
@lru_cache(maxsize=1)
def fetch_s3_client(parallel=True,
                    session=None,
                    session_config=None,
                    worker_data_key="geolab_s3client"):
    """
    Generic tool to fetch s3 client access S3 data.
    
    This function provides a standard tool to get an s3 client efficiently 
    in either a parallel (default) or serial operation.  It behaves 
    totally differently depending on the value or the "parallel" argument. 
    
    Case 1:  parallel == True (default)
    In this case the function assumes an instance of a versions of 
    the S3Worker class (defined in this same module) has been pushed to 
    all workers with the "register_plugin" method of the dask.distributed.Client.
    Different clients with different configurations can be loaded onto 
    workers with different values of a key.  To have this function 
    access an alternative to the default client run the functionw with 
    an appropriate value for `worker_data_key` (see below for supported options).
    
    Case 2:  parallel==False
    Set the parallel argument False if you are using this function in 
    a serial task.  If you are running serial and you do not set parallel
    False function will throw a KeyError exception.  For efficiency 
    this function is pushed to a global cache using the `functools.lru_cache` 
    decorator.   That improves performance as otherwise every time the function 
    is called a new client would have to instantiated from a Session object. 
    Not as bad as starting from nothing but not zero either.   The cache 
    approach makes subsequent access times tiny.  
    
    :param parallel:  boolean that when set True (default) assumes a client 
       has already been pushed to dask workers with register_plugin.   In that 
       mode it will try to fetch that client from the worker with a key 
       defined by the worker_data_key argument.   When False the first 
       time the function is called a client is created but subsequent 
       calls fetch the client cached in memory.
    :param session:   boto3 Session object that should be used to 
       instiate a client.  This should only be used for initializing 
       a client in serial mode.  It  will be silently ignored if 
       parallel is True.  Default is None which causes a default 
       Session object to be created for the cached serial client.  
       Pass a valid value if need to use an alternative.
    :param session_config:  boto3 Config object to specify alternative 
       configurations to default.   Use this only if you know what you are doing.
       It is used only for serial mode and will be silently ignored if used 
       with parallel set True.
    :param worker_data_key:  string key used only when parallel is set True. 
       This parameter defines the key used to access the appropriate 
       instance of s3 client on each worker.   Multiple keys can be used to 
       allow using clients with different configurations for different access
       credentials.   There are currently three distinct plugin implementations 
       with different default key values.  
       
       1.  GeoLabS3Worker uses the key "geolab_s3client" by default.
       2.  AnonymousS3Worker uses the key "anonymous_s3client" by default
       3.  StockS3Worker uses the key "stock_s3client" as default. 
       
       Those keys, however, are not frozen.   When any of the worker plugins 
       are instatiated you can set the value for that key with the 
       common argument "key=".   If you do, the driver code will need to 
       change worker_data_key appropriately as well.  You will need to 
       set worker_data_key running parallel on anywhere but on geolab.  
    """
    if parallel:
        try:
            worker = get_worker()
        except Exception as e:
            raise ValueError(
                "fetch_s3_client: this function must be "
                "executed within a Dask worker context so that get_worker() succeeds and an "
                "s3 client is available via a worker plugin."
            ) from e
        try:
            s3_client = worker.data[worker_data_key]

        except KeyError as e:
            message = "fetch_s3_client:  dask worker has no S3 plugin registered with name={}\n".format(
                worker_data_key
            )
            message += "Register S3Worker before submitting tasks to the dask cluster"
            raise ValueError(message) from e
    else:
        if session is None:
            # This should work as this would be the use for a stock AWS
            # login.   There the credentials are supposed to be cached so this 
            # should work unless proven othewise
            session = boto3.Session()
        if session_config is None:
            # default config frozen for now
            # may need a more flexible way to do this than hard code this
            s3_client = session.client(
                "s3",
                config=Config(
                    request_checksum_calculation="when_required",
                    response_checksum_validation="when_required",
                    ),
                )
        else:
            s3_client = session.client("s3", config=session_config)

    return s3_client


class GeoLabS3Worker(WorkerPlugin):
    """
    Dask worker plugin to create a worker resident client on each dask worker.
    This instance works only with earthscope_sdk used on GeoLab.

    An S3 client requires significant time to construct for a variety of reasons.
    It is known to be a really serious bottleneck if s3 clients are instantiated
    inside a parallel workflow to access s3.   This class can be used to
    create what is called a worker plugin for dask.   The standard use in a
    python script is:
    ```
        dask_client = mspass_client.get_scheduler()
        s3plugin = GeoLabS3Worker()
        dask_client.register_plugin(s3plugin)
    ```
    That creates and loads a memory resident s3 client in each worker's memory space.
    Processing functions that need access to s3 should then use the function
    `fetch_s3_client` to get a reference to the memory resident s3 client.
    That produces near zero overhead in a worker process particularly compared to
    the time needed to instantiate a new instance of an s3 client.
    
    Note this plugin differs from it's siblings in this module as it caches the 
    s3 client as an attribute of this class.  That is a necessary evil to deal with 
    timeout issues in GeoLab that complicate this code.   Since this plugin is 
    only of use on GeoLab this should not matter as one would not expect multiple 
    s3 clients with different credentialsl run on GeoLab simultaneously that 
    would require distinguishing the multiple instances.   
    """

    def __init__(self, key="geolab_s3client"):
        """
        Standard constructor.

        Normal use requires no arguments.  
        """
        # probably unnecessary initializations but makes clear these are two attributes
        # managed by this class
        self.worker_key = key
        self.s3_client = None
        self._cleanup = None


    def setup(self, worker):
        """
        Required method for a worker plugin.  This acts like a secondary constructor.
        It is involked on each worker when this object is pushed to
        each worker with the register_plugin method of the dask client.
        """
        # Gemini's comment on why this complication is needed here:
        # Keep the SDK's refresh provider and real expiration intact.  A frozen
        # credential snapshot has no expiration, and a new SDK session may reuse
        # cached credentials; inventing a new expiry would extend their lifetime
        # locally without extending their actual AWS validity.
        with ExitStack() as cleanup:
            esclient = EarthScopeClient()
            cleanup.callback(esclient.close)
            session = esclient.user.get_boto3_session()
            # use parallel=False because this method is executed on each 
            # worker and to that python instance it is a serial thing
            # confusing though as this always done in a parallel context
            s3_client = fetch_s3_client(parallel=False,session=session)
            # this is a callback to ExitStack which means it is executed only when the 
            # with block exits
            cleanup.callback(s3_client.close)
            # Gemini suggests we should use the construct commented out for reason here.
            # A client is a worker resource, not task data: keeping it in
            # worker.data could ask Dask to pickle/spill its credentials.
            #self.s3_client = s3_client
            # Testing showed that did work running in isolation on GeoLab but I (glp) don't think 
            # what gemini suggests would work if multiple worker clients were defined 
            # this use makes this consistent with other instances of the WorkerPlugin for
            # using s3
            worker.data[self.worker_key] = s3_client
            self._cleanup = cleanup.pop_all()

    def teardown(self, worker):
        """
        Required method for a worker plugin.  This method is effectively a destructor
        called when the class goes out of scope.   It is essential in this case to
        avoid a resource leak as it properly closes the clients connections to s3.
        """
        s3_client = worker.data.get(self.worker_key)
        s3_client.close()
        cleanup = getattr(self, "_cleanup", None)
        if cleanup is not None:
            self._cleanup = None
            #self.s3_client = None
            cleanup.close()

class AnonymousS3Client(WorkerPlugin):
    """
    Dask worker plugin to create a worker resident client on each dask worker.
    This instance works only for anonymous access.  

    An S3 client requires significant time to construct for a variety of reasons.
    It is known to be a really serious bottleneck if s3 clients are instantiated
    inside a parallel workflow to access s3.   This class can be used to
    create what is called a worker plugin for dask.   The standard use in a
    python script is:
    ```
        dask_client = mspass_client.get_scheduler()
        s3plugin = GeoLabS3Worker()
        dask_client.register_plugin(s3plugin)
    ```
    That creates and loads a memory resident s3 client in each worker's memory space.
    Processing functions that need access to s3 should then use the function
    `fetch_s3_client` to get a reference to the memory resident s3 client.
    That produces near zero overhead in a worker process particularly compared to
    the time needed to instantiate a new instance of an s3 client.
    
    Note the default key the fetch_s3_client function needs to use 
    this client is "anonymous_s3client". It can be changed if desired but
    you will need to change calls to fetch_s3_client appropriately if you do.
    """
    def __init__(self,key="anonymous_s3client",region="us-east-2"):
        self.worker_key = key
        self.region = region
    def setup(self,worker):
        s3_client = boto3.client(
                's3',
                region_name=self.region,
                config=Config(signature_version=UNSIGNED)
            )

        worker.data[self.worker_key] = s3_client
    def teardown(self,worker):
        s3_client = worker.data.get(self.worker_key)
        s3_client.close()
        
class StockS3Client(WorkerPlugin):
    """
    Dask worker plugin to create a worker resident client on each dask worker.
    This instance should be used for conventional AWS login accounts where 
    the user has already valid credentials cached in their run environment.  

    An S3 client requires significant time to construct for a variety of reasons.
    It is known to be a really serious bottleneck if s3 clients are instantiated
    inside a parallel workflow to access s3.   This class can be used to
    create what is called a worker plugin for dask.   The standard use in a
    python script is:
    ```
        dask_client = mspass_client.get_scheduler()
        s3plugin = GeoLabS3Worker()
        dask_client.register_plugin(s3plugin)
    ```
    That creates and loads a memory resident s3 client in each worker's memory space.
    Processing functions that need access to s3 should then use the function
    `fetch_s3_client` to get a reference to the memory resident s3 client.
    That produces near zero overhead in a worker process particularly compared to
    the time needed to instantiate a new instance of an s3 client.
    
    Note the default key the fetch_s3_client function needs to use 
    this client is "stock_s3client".  It can be changed if desired but
    you will need to change calls to fetch_s3_client appropriately if you do.
    """
    def __init__(self,key="stock_s3client",region="us-east-2"):
        self.worker_key = key
        self.region = region
    def setup(self,worker):
        # this simple construct assumes credentials are set using
        # the default AWS credentials chain.  In that case the 
        # access keys are cached and this should work according to Gemini
        s3_client = boto3.client(
                's3',
                region_name=self.region,
            )

        worker.data[self.worker_key] = s3_client
    def teardown(self,worker):
        s3_client = worker.data.get(self.worker_key)
        s3_client.close()