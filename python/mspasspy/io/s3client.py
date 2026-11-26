#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prototype worker plugin for using dask on AWS to allow workers to
not have to instantiate an s3 client on each submit.   Modeled after
the mongodb worker plugin.

@author: pavlis
"""

import os
import boto3
from contextlib import ExitStack
from botocore.config import Config
from botocore import UNSIGNED
from functools import lru_cache
from pathlib import Path
from tempfile import TemporaryDirectory
from earthscope_sdk import EarthScopeClient
from earthscope_sdk.config.models import AuthFlowSettings
from earthscope_sdk.config.settings import SdkSettings
from dask.distributed import WorkerPlugin, get_worker


def fetch_s3_client(
    parallel=True,
    session=None,
    session_config=None,
    worker_data_key="earthscope_s3client",
):
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
    False the function will raise a ValueError exception.  For efficiency
    serial client creation is cached using `functools.lru_cache`.  Otherwise
    every call would instantiate a new client from a Session object.
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

       1.  EarthscopeS3Worker uses the key "earthscope_s3client" by default.
       2.  AnonymousS3Worker uses the key "anonymous_s3worker" by default
       3.  StockS3Worker uses the key "stock_s3worker" as default.

       Those keys, however, are not frozen.   When any of the worker plugins
       are instatiated you can set the value for that key with the
       common argument "key=".   If you do, the driver code will need to
       change worker_data_key appropriately as well.
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
            if (
                worker_data_key == "earthscope_s3client"
                and "geolab_s3client" in worker.data
            ):
                return worker.data["geolab_s3client"]
            message = "fetch_s3_client:  dask worker has no S3 plugin registered with name={}\n".format(
                worker_data_key
            )
            message += "Register S3Worker before submitting tasks to the dask cluster"
            raise ValueError(message) from e
    else:
        s3_client = _serial_s3_client(session, session_config)

    return s3_client


def _create_s3_client(session=None, session_config=None):
    """
    Internal function used to instatiatge a serial client from a
    boto3 Session and Config object.  It a pure convenciece function
    to reduce repetitous code.
    """
    if session is None:
        session = boto3.Session()
    if session_config is None:
        session_config = Config(
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        )
    return session.client("s3", config=session_config)


@lru_cache(maxsize=1)
def _serial_s3_client(session=None, session_config=None):
    """
    Thin wrapper used in combination with fetch_s3_client to
    efficiently create an s3 client.  It is a necessary evil to
    exploit the chaching method using the lru_cache decorator.
    """
    return _create_s3_client(session, session_config)


# Keep the public cache controls while caching only serial clients.  Worker
# lookups must reflect the current worker and its current plugin registration.
fetch_s3_client.cache_clear = _serial_s3_client.cache_clear
fetch_s3_client.cache_info = _serial_s3_client.cache_info


class EarthscopeS3Worker(WorkerPlugin):
    """
    Dask worker plugin to create a worker resident client on each dask worker.
    This version is used for access to Earthscope S3 archives outside the
    open data collection.  Use it to access any earthscope data via their
    SDK that is not open data.  It handles credentials with the special
    earthscope mechanism.

    An S3 client requires significant time to construct for a variety of reasons.
    It is known to be a really serious bottleneck if s3 clients are instantiated
    inside a parallel workflow to access s3.   This class can be used to
    create what is called a worker plugin for dask.   The standard use in a
    python script is:
    ```
        dask_client = mspass_client.get_scheduler()
        s3plugin = EarthscopeS3Worker()
        dask_client.register_plugin(s3plugin)
    ```
    That creates and loads a memory resident s3 client in each worker's memory space.
    Processing functions that need access to s3 should then use the function
    `fetch_s3_client` to get a reference to the memory resident s3 client.
    That produces near zero overhead in a worker process compared to
    the time needed to instantiate a new instance of an s3 client.

    Resolve credentials on the driver before registration. Dask serializes the
    resolved credentials to its scheduler and workers, so register this plugin
    only with a trusted cluster. Log in again and re-register the plugin when
    the driver's credentials need to be replaced. Environment tokens use the
    SDK's default public OAuth application. A driver SDK profile can supply
    public OAuth settings for refreshing its tokens on workers.
    """

    def __init__(self, key="earthscope_s3client", profile_name=None):
        self.worker_key = key
        self.s3_client = None
        self._cleanup = None

        self._oauth2_options = {}
        refresh_token = os.environ.get("ES_OAUTH2__REFRESH_TOKEN", "").strip()
        if refresh_token:
            # Never combine an explicit refresh token with a stale access
            # token from another identity in the driver's SDK profile.
            access_token = os.environ.get("ES_OAUTH2__ACCESS_TOKEN", "").strip()
            self.profile_name = (
                profile_name or os.environ.get("ES_PROFILE") or "default"
            )
        else:
            settings = (
                SdkSettings(profile_name=profile_name)
                if profile_name is not None
                else SdkSettings()
            )
            self.profile_name = settings.profile_name
            oauth2 = settings.oauth2
            # Refresh tokens are bound to the issuing OAuth application.
            self._oauth2_options = {
                "client_id": oauth2.client_id,
                "domain": str(oauth2.domain),
                "scope": oauth2.scope,
                "audience": oauth2.audience,
                "allowed_hosts": oauth2.allowed_hosts,
            }
            sdk_refresh = oauth2.refresh_token
            sdk_access = oauth2.access_token
            refresh_token = (
                sdk_refresh.get_secret_value().strip() if sdk_refresh else ""
            )
            access_token = sdk_access.get_secret_value().strip() if sdk_access else ""
        if not refresh_token:
            raise RuntimeError(
                "No EarthScope refresh token found. Set ES_OAUTH2__REFRESH_TOKEN "
                "or run `es login` before registering EarthscopeS3Worker with Dask."
            )
        self._refresh_token = refresh_token
        self._access_token = access_token or None

    def setup(self, worker):
        """
        Secondary constructor for Earthscope client setup.

        A dask worker plugin requires this method.  It acts as a
        secondary constructor which in this case actually instantiates
        an instance of an EarthscopeClient which is pushed to the
        workers.

        Extra complexity enters in this plugin to handle the special
        way Earthscope handles access credentials.   See the user manual
        and online sources to understand that complex process.
        To understand this code you need to realize the incantations
        here load the two keys called "access_token" and "refresh_token"
        in a way that they can be serialized and loaded into the
        workers data area.
        """
        with ExitStack() as cleanup:
            # Copy resolved driver settings without loading worker-local settings.
            # Separate SDK token and AWS grant caches for each plugin instance.
            profile_dir = Path(
                cleanup.enter_context(TemporaryDirectory(prefix="mspass-earthscope-"))
            )
            worker_settings = SdkSettings.model_construct(
                profile_name=self.profile_name,
                oauth2=AuthFlowSettings(
                    refresh_token=self._refresh_token,
                    access_token=self._access_token,
                    **self._oauth2_options,
                ),
            ).model_copy(
                update={
                    "profile_dir": profile_dir,
                    "tokens_file": profile_dir / "tokens.json",
                }
            )
            esclient = EarthScopeClient(settings=worker_settings)
            cleanup.callback(esclient.close)
            session = esclient.user.get_boto3_session()
            s3_client = _create_s3_client(session=session)
            cleanup.callback(s3_client.close)

            worker.data[self.worker_key] = s3_client
            self.s3_client = s3_client
            self._cleanup = cleanup.pop_all()

    def teardown(self, worker):
        """
        This method acts like a destructor for the class.
        It is nontrivial as it has to close up any active AWS connections
        and clear the worker data area where the plugin was loaded.
        """
        if worker.data.get(self.worker_key) is self.s3_client:
            worker.data.pop(self.worker_key, None)
        cleanup = self._cleanup
        self._cleanup = None
        self.s3_client = None
        if cleanup is not None:
            cleanup.close()


class GeoLabS3Worker(EarthscopeS3Worker):
    """Backward-compatible name and default registration key."""

    def __init__(self, key="geolab_s3client", profile_name=None):
        super().__init__(key=key, profile_name=profile_name)


class AnonymousS3Worker(WorkerPlugin):
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
        s3plugin = AnonymousS3Worker()
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

    def __init__(self, key="anonymous_s3worker", region="us-east-2"):
        self.worker_key = key
        self.region = region
        self.s3_client = None

    def setup(self, worker):
        """
        Secondary constructor.

        Like all plugins this required method sets up the actual
        process.  This one instantiates an open data s3 access client
        which is trivial compared to any other s3 client.
        """
        s3_client = boto3.client(
            "s3", region_name=self.region, config=Config(signature_version=UNSIGNED)
        )

        try:
            worker.data[self.worker_key] = s3_client
        except Exception:
            s3_client.close()
            raise
        self.s3_client = s3_client

    def teardown(self, worker):
        """
        This method acts like a destructor for the class.
        It is nontrivial as it has to close up any active AWS connections
        and clear the worker data area where the plugin was loaded.
        """
        s3_client = self.s3_client
        self.s3_client = None
        if s3_client is not None:
            if worker.data.get(self.worker_key) is s3_client:
                worker.data.pop(self.worker_key, None)
            s3_client.close()


class StockS3Worker(WorkerPlugin):
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
        s3plugin = StockS3Worker()
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

    WARNING:   this plugin is not as well tested as the other plugins in
    this module.
    """

    def __init__(self, key="stock_s3worker", region="us-east-2"):
        self.worker_key = key
        self.region = region
        self.s3_client = None

    def setup(self, worker):
        # this simple construct assumes credentials are set using
        # the default AWS credentials chain.  In that case the
        # access keys are cached and this should work according to Gemini
        s3_client = boto3.client(
            "s3",
            region_name=self.region,
        )

        try:
            worker.data[self.worker_key] = s3_client
        except Exception:
            s3_client.close()
            raise
        self.s3_client = s3_client

    def teardown(self, worker):
        s3_client = self.s3_client
        self.s3_client = None
        if s3_client is not None:
            if worker.data.get(self.worker_key) is s3_client:
                worker.data.pop(self.worker_key, None)
            s3_client.close()
