#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 28 09:15:40 2025

@author: pavlis
"""
from abc import ABC, abstractmethod
import math
import os
import platform
import re
import shlex
import subprocess
import time
from urllib.parse import urlsplit

from distributed import Client
from pymongo import MongoClient
import yaml

class BasicMsPASSLauncher(ABC):
    """
    Base class constructor loads common attribute from a yaml file.
    
    The base class should read attributes to be set in self that are 
    common to all superclasses.  Superclasses should read the same file and 
    parse additional attributes not in the base class.
    
    For convenience the dictionary created from the yaml file is 
    stored as self.yaml.dict.   That allows superclasses to not have 
    reload the yaml file by running super()._init__ with a yaml 
    file.   When that is done additional attributes can be parsed from 
    self.yaml_dict.
    """
    def __init__(self,
                 configuration_file=None,
                 ):
        """
        Base class constructor loading core attributes. 
        
        This constructor loads common attributes or superclass launchers.
        The expectation is superclasses will normally contain a file-based 
        constructor and the thing superclasses do is call this 
        method with the super().__init__ python idiom.
        
        :param configuration file:  file name of yaml file to 
          to loaded.   Note this string should normally be a 
          file in the working directory which the python interpetter 
          instantiating an instance of this class is run.  Alternatively 
          you can specify a full path for the file.  In that case
          the function will detect that fact and use that full path. 
          If undefined (None) an Antelope like approach is tried wherein 
          the constructor will check if the env MSPASS_HOME is defined 
          and if it is it looks there for a file called "mspass_cluster.yaml". 
          If MSPASS_HOME is not defined, it checks for the default file 
          name ("mspass_cluster.yaml") in ../data/yaml.
        """
        self.yaml_dict = self._parse_yaml_file(configuration_file)
        self.container = self.yaml_dict['container']
        self.working_directory = self.yaml_dict['working_directory']
        self.log_directory = self.yaml_dict['log_directory']
        self.database_directory = self.yaml_dict['database_directory']
        self.worker_directory = self.yaml_dict['worker_directory']
        self.workers_per_node = self.yaml_dict['workers_per_node']
        self.primary_node_workers = self.yaml_dict['primary_node_workers']
        self.cluster_subnet_name = self.yaml_dict['cluster_subnet_name']
        
        
    def _parse_yaml_file(self,filename=None)->dict:
        """
        Parses the yaml configuration file for this class and returns 
        the result as  python dictionary.  The dictionary returned 
        is saved as a self variable so superclasses can parse additional 
        attributes without the baggage of reading and parsing the file 
        again.   A bit unusual but workable in this case because 
        configuration files are never exected to be large so storing the 
        image is not a memory problem. 
        """
        # this was derived from a similar parsing for schema.py
        if filename is None:
            if "MSPASS_HOME" in os.environ:
                config_file = (
                    os.path.abspath(os.environ["MSPASS_HOME"])
                    + "/data/yaml/mspass_cluster.yaml"
                )
            else:
                config_file = os.path.abspath(
                    os.path.dirname(__file__) + "/../data/yaml/mspass_cluster.yaml"
                )
        elif os.path.isfile(filename):
            config_file=filename
        else:
            if "MSPASS_HOME" in os.environ:
                config_file = os.path.join(
                    os.path.abspath(os.environ["MSPASS_HOME"]), "data/yaml", filename
                )
            else:
                config_file = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "../data/yaml", filename)
                )
                
        try:
            with open(config_file, "r") as stream:
                result_dic = yaml.safe_load(stream)
            return result_dic
        except yaml.YAMLError as e:
            print(f"Failure parsing configuration file={config_file}")
            print(f"Message posted: {e}")
            raise RuntimeError("HPCClusterLauncher Constructor failed")
        except EnvironmentError as e:
            print(f"Open failed on yaml file={config_file}")
            print(f"Message posted: {e}")
            raise RuntimeError("HPCClusterLauncher Constructor failed")
        except Exception as e:
            print(f"Unexpected exception thrown by yaml.safe_load")
            print(f"Message posted: {e}")
            raise RuntimeError("HPCClusterLauncher Constructor failed")

        
    @abstractmethod
    def launch(self):
        """
        Concrete implementations should implement this method that 
        launches all the required MsPASS components.   
        """
        pass
    @abstractmethod
    def status(self):
        """
        Concrete implementations should implement this method that 
        returns some form of status information that a master script 
        can use to verify all the mspass components are functioning.  
        """
        pass
    
    @abstractmethod
    def run(self,python_file):
        """
        Concrete implementations should implement this method that 
        runs the scritp in python_file on the MsPASS cluster managed 
        by the object.  
        """
        pass
    
class HPCClusterLauncher(BasicMsPASSLauncher):
    """
    Launcher to run on an HPC cluster.   
    
    This class provides a mechanism to run a containerized version of 
    MsPASS on an HPC system.   It is known to work only with slurm 
    job scheduling but alternative schedulers should be possible by 
    changing only the configuratin file.   The job schduler enter only 
    in tryig to grok the list of compute nodes assigned to a job.   
    
    This class acts similar to shell-script launcers for HPC developed 
    at TACC.   By using python, however, is is more configurable and and 
    has some added features.  There are currently three major enhancement this 
    implmntation adds oaver the shell script approach:
        1.  The same laucher works for single node and multimode jobs 
            with the same configuration.  It does that by automatically 
            launching workers on the primary node if slurm says there is 
            only one node allocated for the job. 
        2.  The containers are managed much more cleanly as subprocesses 
            spawned on the primary node by an instance of this launcher.  
            That provides a cleaner exit when the job finishes.  
        3.  It extends the base class by adding a "terminate" method 
            which can be used to have the containers exit gracefully. 
            It also provides a mechanism to relaunch a cluster in a 
            different configuration in the middle of a job.  Not as helpful 
            as it could be with slurm because resources are allocated at the 
            start of the job and are fixed for the duration of "a job". 
    """
    def __init__(self,
                 configuration_file=None,
                 auto_launch=True,
                 verbose=False,
                 ):
        """
        Build an instance of this class from a yaml file.
        
        This classes uses the base class construtor to parse the 
        actual yaml file.  It expects to find a dictonary 
        it can fetch with the key "HPC_cluster" containing attributes 
        specific to this class.  (what MongoDB would call a subdocument)
        That approach more cleanly separates what attributes are 
        needed only by this superclass.  It also allows alternative 
        implementations that are variants of this to be used with the 
        same site-specific configuration file with alternative keys. 
        i.e. a user should feel free to implement a variant of this 
        launcher but the key.  Then this or the alternative can be 
        run using a common configuration files.  
        
        Key-value pairs in the yaml file that define the configuation 
        to use are best documented separately.  Where that document will 
        live is to be determined.  It will be either a user manual change 
        for hpc setup or a readme file in the mspass scripts directory.
        
        :param configuration file:  file name of yaml file to 
          to loaded.   Note this string should normally be a 
          file in the working directory which the python interpetter 
          instantiating an instance of this class is run.  Alternatively 
          you can specify a full path for the file.  In that case
          the function will detect that fact and use that full path. 
          If undefined (None) an Antelope like approach is tried wherein 
          the constructor will check if the env MSPASS_HOME is defined 
          and if it is it looks there for a file called "mspass_cluster.yaml". 
          If MSPASS_HOME is not defined, it checks for the default file 
          name ("mspass_cluster.yaml") in ../data/yaml.  
        :type coniguration_file: string or Null (see above)
        :param auto_launch:   boolean that when set True (default) will 
          call the `launch` method if the construtor completes without error. 
          This is the default as it makes the object follow the common
          OOP recommendation that "constrution is initialization"  
          Similarly the object as a destuctor defined that automatically 
          releases resources the object manages (in this case the containerized 
          componnts) when it goes out of scope. 
        :param verbose:  When True print out information useful for 
          debugging a configuration issue.   Use when setting up 
          a new configuration to verify it is what you want. 
        
        """
        message0 = "HPCClusterLauncher constructor:  "
        if verbose:
            print("Loading configuration file=", configuration_file)
        super().__init__(configuration_file)
        # The base class constructor creates this image of the yaml 
        # file.  It only extracts common attributes.  Here we 
        # translate that external representation to attributes needed 
        # for this concrete implementation
        cluster_config = self.yaml_dict["HPC_cluster"]
        self.container_run_command = cluster_config["container_run_command"]
        self.container_run_args = cluster_config["container_run_args"]
        self.container_env_flag = cluster_config["container_env_flag"]
        # at present this is local version of mpiexec
        self.worker_run_command = cluster_config["worker_run_command"]
        self.task_scheduler = cluster_config["task_scheduler"]

        self.scheduler_process = None
        self.dbserver_process = None
        self.primary_worker_process = None
        self.remote_worker_process = None
        self.jupyter_process = None

        js = cluster_config["job_scheduler"]
        if js != "slurm":
            message = message0
            message += "Cannot handle job_scheduler={}\n".format(js)
            message += "Currently only support slurm"
            raise ValueError(message)

        if verbose:
            print("job scheduler set as slurm")
        primary_setting = cluster_config["primary_host"]
        database_setting = cluster_config["database_host"]
        scheduler_setting = cluster_config["scheduler_host"]
        worker_setting = cluster_config["worker_hosts"]
        needs_discovery = any(
            value == "auto"
            for value in (
                primary_setting,
                database_setting,
                scheduler_setting,
                worker_setting,
            )
        )
        hostlist = []
        if needs_discovery:
            completed = subprocess.run(
                ["scontrol", "show", "hostname"],
                capture_output=True,
                text=True,
            )
            hostlist = [
                host.strip() for host in completed.stdout.split() if host.strip()
            ]
            if not hostlist:
                if self.primary_node_workers == 0:
                    raise RuntimeError(
                        message0
                        + "scontrol command yielded an empty list of hostnames\n"
                        + "Cannot continue"
                    )
                completed = subprocess.run(["hostname"], capture_output=True, text=True)
                hostname = completed.stdout.strip()
                if not hostname:
                    raise RuntimeError(
                        message0 + "hostname command returned no hostname"
                    )
                hostlist = [hostname]

        self.primary_node = (
            hostlist[0] if primary_setting == "auto" else primary_setting
        )
        self.database_host = (
            self.primary_node if database_setting == "auto" else database_setting
        )
        self.scheduler_host = (
            self.primary_node if scheduler_setting == "auto" else scheduler_setting
        )
        if worker_setting == "auto":
            self.worker_hosts = [host for host in hostlist if host != self.primary_node]
        elif isinstance(worker_setting, str):
            self.worker_hosts = [worker_setting] if worker_setting else []
        else:
            self.worker_hosts = list(worker_setting)

        if not self.worker_hosts and self.primary_node_workers == 0:
            raise RuntimeError(
                message0
                + "no remote workers were configured and primary_node_workers is 0"
            )
        if verbose:
            print("Primary node name=", self.primary_node)
            print("database hostname=", self.database_host)
            print("scheduler hostname=", self.scheduler_host)
            print("Worker hostname(s)=", self.worker_hosts)

        if cluster_config["setup_tunnel"]:
            tunnel_args = shlex.split(cluster_config["tunnel_setup_command"])
            tunnel_args.append(self.primary_node)
            subprocess.run(tunnel_args, capture_output=True, text=True, check=True)

        if auto_launch:
            self.launch(verbose=verbose)

    def __del__(self):
        """
        Class destructor. 
        
        The destrutor is called when an object goes out of scope. 
        This instance is little more than a call to self.shutdown()
        which shuts down all the containers as gracefully as possible.  
        """
        try:
            self.shutdown()
        except Exception:
            pass

    @staticmethod
    def _startup_settings():
        try:
            timeout = float(os.environ.get("MSPASS_STARTUP_TIMEOUT_SECONDS", "120"))
            poll_interval = float(os.environ.get("MSPASS_STARTUP_POLL_SECONDS", "2"))
        except ValueError as error:
            raise ValueError(
                "HPCClusterLauncher startup timeout and poll values must be numbers"
            ) from error
        if (
            not math.isfinite(timeout)
            or timeout <= 0.0
            or not math.isfinite(poll_interval)
            or poll_interval <= 0.0
        ):
            raise ValueError(
                "HPCClusterLauncher startup timeout and poll values must be finite and positive"
            )
        return timeout, poll_interval

    @staticmethod
    def _popen(args):
        return subprocess.Popen(args, close_fds=True)

    @staticmethod
    def _scheduler_endpoint(address):
        endpoint = address if "://" in address else "tcp://" + address
        parsed = urlsplit(endpoint)
        if parsed.port is None:
            port = os.environ.get("DASK_SCHEDULER_PORT") or "8786"
            parsed = parsed._replace(netloc=parsed.netloc + ":" + port)
        return parsed.geturl()

    @staticmethod
    def _stop_process(process):
        if process is None:
            return
        if process.poll() is None:
            process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)

    def _cleanup_owned_processes(self):
        first_error = None
        for attribute in (
            "jupyter_process",
            "primary_worker_process",
            "remote_worker_process",
            "dbserver_process",
            "scheduler_process",
        ):
            process = getattr(self, attribute, None)
            try:
                self._stop_process(process)
            except Exception as error:
                if first_error is None:
                    first_error = error
            else:
                setattr(self, attribute, None)
        if first_error is not None:
            raise first_error

    def _raise_startup_error(self, message):
        try:
            self._cleanup_owned_processes()
        except Exception as cleanup_error:
            message += "; cleanup failed: {}".format(cleanup_error)
        raise RuntimeError(message)

    def _require_running(self, name, process):
        if process is None:
            self._raise_startup_error(
                "HPCClusterLauncher: {} was not started".format(name)
            )
        status = process.poll()
        if status is not None:
            self._raise_startup_error(
                "HPCClusterLauncher: {} exited during startup with code {}".format(
                    name, status
                )
            )

    def _probe_database(self):
        client = MongoClient(self.database_host, serverSelectionTimeoutMS=2000)
        try:
            client.admin.command("ping")
        finally:
            client.close()

    def _probe_scheduler(self):
        client = Client(self._scheduler_endpoint(self.scheduler_host), timeout="2s")
        try:
            client.scheduler_info()
        finally:
            client.close()

    def _wait_for_services(self):
        timeout, poll_interval = self._startup_settings()
        deadline = time.monotonic() + timeout
        while True:
            self._require_running("scheduler", self.scheduler_process)
            self._require_running("database", self.dbserver_process)
            database_ready = False
            scheduler_ready = False
            try:
                self._probe_database()
                database_ready = True
            except Exception:
                pass
            try:
                self._probe_scheduler()
                scheduler_ready = True
            except Exception:
                pass
            if database_ready and scheduler_ready:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0.0:
                self._raise_startup_error(
                    "HPCClusterLauncher: services did not become ready within {} seconds".format(
                        timeout
                    )
                )
            time.sleep(min(poll_interval, remaining))

    def _container_args(self, environment):
        return self._initialize_container_runargs() + [
            self.container_env_flag,
            ",".join(environment),
            self.container,
        ]

    def launch(self, verbose=False):
        """
        Call this method to launch all the MsPASS containized components.
        
        The MsPASS framework requires three containerized components to 
        be running to work correctly:  (1) scheuler, (2) workers, and (3)
        and instance of MongoDB.  This method launches those components using 
        instructions parsed from a configuration file when the object is 
        constructed.   The coponent are spawned as subprocesses from the 
        primary node with the subprocess.Popen function.   That runs the 
        containers in the background with process information cached in this 
        object as self attibutes called "self.scheduler_process",
        "self.dbserver_process", and "self.remote_worker_process".  
        If workers are run on theh primary there will also be a defined 
        valued for "self.primary_worker_process".

        :raises ValueError: if the startup timeout or polling configuration is
          not a finite positive number.  Validation happens before any child
          is started.
        :raises RuntimeError: if an owned child exits early, services do not
          become ready before the deadline, or failure cleanup itself fails.
          An underlying process-creation exception is re-raised unchanged
          after already-started owned children have been cleaned up.
        """
        # Validate these settings before starting any process.  Otherwise an
        # invalid value would leave the scheduler and database children alive.
        self._startup_settings()
        try:
            self.scheduler_process = self._popen(
                self._container_args(
                    [
                        "MSPASS_ROLE=scheduler",
                        "MSPASS_WORK_DIR={}".format(self.working_directory),
                        "MSPASS_SCHEDULER={}".format(self.task_scheduler),
                        "MSPASS_SCHEDULER_ADDRESS={}".format(self.scheduler_host),
                    ]
                )
            )
            self._require_running("scheduler", self.scheduler_process)
            self.dbserver_process = self._popen(
                self._container_args(
                    [
                        "MSPASS_ROLE=db",
                        "MSPASS_WORK_DIR={}".format(self.working_directory),
                        "MSPASS_DB_DIR={}".format(self.database_directory),
                    ]
                )
            )
            self._require_running("database", self.dbserver_process)
            self._wait_for_services()
            worker_args = self._build_worker_run_args()
            if worker_args:
                self.remote_worker_process = self._popen(worker_args)
                self._require_running("remote worker", self.remote_worker_process)
            if self.primary_node_workers > 0:
                self.primary_worker_process = self._popen(
                    self._container_args(
                        [
                            "MSPASS_ROLE=worker",
                            "MSPASS_WORK_DIR={}".format(self.working_directory),
                            "MSPASS_SCHEDULER_ADDRESS={}".format(self.scheduler_host),
                            "MSPASS_DB_ADDRESS={}".format(self.database_host),
                            "MSPASS_WORKER_ARG=--nworkers={} --nthreads 1".format(
                                self.primary_node_workers
                            ),
                        ]
                    )
                )
                self._require_running("primary worker", self.primary_worker_process)
            if verbose:
                print("Successfully launched MongoDB, scheduler, and workers")
        except RuntimeError:
            raise
        except Exception as error:
            try:
                self._cleanup_owned_processes()
            except Exception as cleanup_error:
                raise RuntimeError(
                    "HPCClusterLauncher launch failed: {}; cleanup failed: {}".format(
                        error, cleanup_error
                    )
                ) from error
            raise

    def shutdown(self):
        """Stop every owned child.

        A process handle is cleared only after that process is stopped.  If a
        stop fails, cleanup continues for the other children, the first error
        is re-raised, and the failed handle remains available for a retry.
        """
        self._cleanup_owned_processes()

    def run(self, pyscript):
        """
        Runs pyscript the primary node using this cluster.
        
        This method runs a python script on the primary node. 
        It always runs in batch mode and assumes a python script 
        s the input.  We need a different method to run jupyter 
        notebooks.  Blocks until the script exits.  
        """
        # this can be made more elaborate.  Here I just run 
        # a script
        print("Trying to run python script file=", pyscript)
        self._wait_for_services()
        runline = self._container_args(
            [
                "MSPASS_ROLE=frontend",
                "MSPASS_WORK_DIR={}".format(self.working_directory),
                "MSPASS_DB_ADDRESS={}".format(self.database_host),
                "MSPASS_SCHEDULER_ADDRESS={}".format(self.scheduler_host),
            ]
        ) + ["--batch", pyscript]
        runout = subprocess.run(runline, capture_output=True, text=True)
        print("stdout from this job")
        print(runout.stdout)
        print("stderr from this job")
        print(runout.stderr)

    def interactive_session(self):
        """
        Use this method to launch the jupyter server to initiate an 
        interactive session.  Will print the output from jupyter 
        when it launches to use current cut-paste method to connect to 
        the jupyter server.   
        """
        print("Launching frontend container running juptyer server")
        print("Use cut-and-paste of url printed below to connect")
        self._wait_for_services()
        runline = self._container_args(
            [
                "MSPASS_ROLE=frontend",
                "MSPASS_WORK_DIR={}".format(self.working_directory),
                "MSPASS_DB_ADDRESS={}".format(self.database_host),
                "MSPASS_SCHEDULER_ADDRESS={}".format(self.scheduler_host),
            ]
        )
        self.jupyter_process = self._popen(runline)
        self._require_running("frontend", self.jupyter_process)
        return self.jupyter_process

    def status(self, container="all", verbose=True) -> int:
        """
        Check the status of one or more of the containers managed by this object.
        
        We often need to know if a container is still running.   This method 
        allows one to check if the required contaienrs to run mspass are 
        running.  By default it checks all containers.  One can ask for only 
        one using one of the key strings this function uses to define the 
        instance of the mspass container.  Valid values for arg0 are:
            "all" - check all
            "db"  - check only the container running MongoDB
            "scheduler" - check only the contaner running the dask or
                or spark scheduler
            "primary_worker" - check status of the worker container running on 
                the primary node.
            "remote_worker" - check the worker launcher for remote nodes.
            "frontend" - check the interactive frontend, when one was launched.
                
        Any other values for arg0 will cause this method to throw a 
        ValueError exception.
        
        :param container: container keywords noted above for arg0.  i.e. 
           must be one of "db", "scheduler", "primary_worker", "remote_worker",
           "frontend", or "all" (default)
        :type container:  string
        :param verbose:  boolean that when True (default) uses print to 
           post a status message for container(s) requested.  When false 
           prints nothing and assumes the return will be handled
        :return:  int status.  1 means the container(s) tested were all 
           running.  0 means one or more have died.
        """
        all_containers = [
            "db",
            "scheduler",
            "primary_worker",
            "remote_worker",
            "frontend",
        ]
        if container == "all":
            statlist = all_containers
        else:
            if container in all_containers:
                statlist = [container]
            else:
                message = "HPCClusterLauncher.status:  component={}".format(container)
                message += " invalid\n"
                message += "Must be one of: "
                for c in all_containers:
                    message += c + " "
                raise ValueError(message)

        def verbose_message(container_name, poll_return):
            if poll_return is None:
                print(container_name, " is running")
            else:
                print(container_name, " has exited with code=", poll_return)

        retval = 1
        for container in statlist:
            match container:
                case "db":
                    process = self.dbserver_process
                case "scheduler":
                    process = self.scheduler_process
                case "primary_worker":
                    process = self.primary_worker_process
                case "remote_worker":
                    process = self.remote_worker_process
                case "frontend":
                    process = self.jupyter_process
            if process is None:
                if container in ("db", "scheduler"):
                    retval = 0
                continue
            stat = process.poll()
            if verbose:
                verbose_message(container, stat)
            if stat is not None:
                retval = 0

        return retval

    def _initialize_container_runargs(self) -> list:
        """
        This private method creates the initial list of args 
        used to run a container driven by two key-value pairs 
        in the configuration file:  "container_run_command" and 
        "conainer_run_args".   There are two because the first is 
        commonly just "apptaier run" while the second may contain 
        optional run args like bind arguments.   Note in this 
        class environment variables are always handled separately.
        
        Returns a list that is is the starting point for the list of 
        args used for subprocess.run and subprocess.Popen. 
        """
        return shlex.split(self.container_run_command) + shlex.split(
            self.container_run_args
        )

    def _build_worker_run_args(self) -> list:
        """
        Private method that constructs the command to launch 
        workers on nodes other than the primary node.   Uses the 
        list of hostnames loaded by the contructor.
            
        This function is actually totally married to mpiexec as 
        the args it constructs are only for that application
            
        Returns an empty list if the worker list is empty.
        Caller should handle tha situation and exit if the 
        here are no workers assigned to primary.
        """
        nnodes = len(self.worker_hosts)
        if nnodes == 0:
            return []
        # cthis allows args to be entered on teh run line in config file
        arglist = shlex.split(self.worker_run_command)
        # these are actually locked to mpiexec so this isn't 
        # as flexible as it might look
        arglist.append("-n")
        arglist.append(str(nnodes))
        arglist.append("-ppn")
        arglist.append("1")
        arglist.append("-hosts")
        for hostname in self.worker_hosts:
            arglist.append(hostname)
        # simillar to launch method to generate run  line for container
        arglist.extend(self._initialize_container_runargs())
        # apptainer mthod for setting environment variables loaded 
        # in contaer
        arglist.append(self.container_env_flag)
        arglist.append(
            ",".join(
                [
                    "MSPASS_ROLE=worker",
                    "MSPASS_WORK_DIR={}".format(self.working_directory),
                    "MSPASS_SCHEDULER_ADDRESS={}".format(self.scheduler_host),
                    "MSPASS_DB_ADDRESS={}".format(self.database_host),
                    "MSPASS_WORKER_ARG=--nworkers={} --nthreads 1".format(
                        self.workers_per_node
                    ),
                ]
            )
        )
        arglist.append(self.container)
        return arglist


class DesktopLauncher(BasicMsPASSLauncher):
    """
    Launch a complete Docker Compose MsPASS stack on a desktop.

    This launcher manages the database, scheduler, worker, and frontend
    services defined by the selected Compose file.  It differs from the HPC
    launchers by using Docker Compose instead of Apptainer and by opening the
    Jupyter frontend in a local browser.  Construction launches or attaches to
    the stack, waits until every configured service is running and the
    frontend publishes a real HTTP(S) URL, and then opens that URL.

    The launcher records whether it started the Compose project.  ``shutdown``
    brings down only a project started by this object; a project that was
    already fully or partially running is treated as caller-owned.  Missing
    services in a partial project are started without transferring ownership.
    Browser processes started by this object are always reaped.  Compose
    execution failures are reported as ``RuntimeError``; invalid constructor
    arguments are reported as ``ValueError``.
    """

    _FRONTEND_SERVICE = "mspass-frontend"

    def __init__(
        self,
        configuration="data/yaml/compose.yaml",
        host_os=None,
        browser="FireFox",
        verbose=True,
    ):
        """
        Constructor for DesktopLauncher.

        This implementation uses ``docker compose`` and immediately calls
        :meth:`launch`.  Startup timeout and polling interval are controlled by
        the positive finite environment values
        ``MSPASS_STARTUP_TIMEOUT_SECONDS`` (default 120) and
        ``MSPASS_STARTUP_POLL_SECONDS`` (default 2).

        :param configuration:  yaml file defining the docker compose
          configuration to launch containers.  See User Manual section
          title "Deply MsPASS with docker compose".
        :type configuration: string  (must be a file name ending in ".yaml" or ".yml")
        :param host_os: canonical operating-system name used to construct the
          browser command.  ``None`` selects ``platform.system()``.  Supported
          values are ``Linux``, ``Darwin``, and ``Windows``.
        :type host_os: string or None
        :param browser: browser executable or macOS application name.
        :type browser: string
        :param verbose:  boolean controling if the constructor print launch output.
          When False runs silently unless there is an exception.  When True the
          output of docker compose is captured and echoed to stdout of the
          calling python script.
        """
        self.configuration_file = configuration
        self.host_os = platform.system() if host_os is None else host_os
        self.browser = browser
        self.verbose = verbose
        self.browser_process = None
        self._owns_stack = False
        self._url = None
        self._startup_timeout = self._positive_environment_value(
            "MSPASS_STARTUP_TIMEOUT_SECONDS", 120.0
        )
        self._startup_poll = self._positive_environment_value(
            "MSPASS_STARTUP_POLL_SECONDS", 2.0
        )
        if self.host_os not in ("Linux", "Darwin", "Windows"):
            raise ValueError(
                "DesktopLauncher does not support host operating system "
                + repr(self.host_os)
            )
        self.launch()

    @classmethod
    def _positive_environment_value(cls, name, default):
        value_string = os.environ.get(name, str(default))
        try:
            value = float(value_string)
        except (TypeError, ValueError):
            raise ValueError(f"{name} must be a finite positive number")
        if not math.isfinite(value) or value <= 0.0:
            raise ValueError(f"{name} must be a finite positive number")
        return value

    def _compose_argv(self, *arguments):
        return [
            "docker",
            "compose",
            "-f",
            self.configuration_file,
            *arguments,
        ]

    def _run_compose(self, *arguments):
        argv = self._compose_argv(*arguments)
        try:
            result = subprocess.run(argv, capture_output=True, text=True)
        except Exception as error:
            raise RuntimeError(
                "DesktopLauncher failed to execute " + " ".join(argv) + f": {error}"
            ) from error
        if self.verbose:
            if result.stdout:
                print(result.stdout, end="" if result.stdout.endswith("\n") else "\n")
            if result.stderr:
                print(result.stderr, end="" if result.stderr.endswith("\n") else "\n")
        if result.returncode != 0:
            message = result.stderr.strip() or result.stdout.strip()
            raise RuntimeError(
                "DesktopLauncher command failed: "
                + " ".join(argv)
                + (f": {message}" if message else "")
            )
        return result

    def _browser_argv(self, url):
        if self.host_os == "Linux":
            return [self.browser, url]
        if self.host_os == "Darwin":
            return ["open", "-a", self.browser, url]
        return ["cmd", "/c", "start", "", self.browser, url]

    def _stop_browser(self):
        process = self.browser_process
        if process is None:
            return
        try:
            browser_running = process.poll() is None
        except Exception:
            browser_running = True
        if browser_running:
            process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
        self.browser_process = None

    def _cleanup_after_failed_launch(self):
        errors = []
        try:
            self._stop_browser()
        except BaseException as error:
            errors.append(str(error))
        if self._owns_stack:
            try:
                self._run_compose("down")
            except BaseException as error:
                errors.append(str(error))
            else:
                self._owns_stack = False
        self._url = None
        return errors

    def _wait_for_url(self):
        deadline = time.monotonic() + self._startup_timeout
        while True:
            logs = self._run_compose("logs", self._FRONTEND_SERVICE)
            url = extract_jupyter_url(logs.stdout)
            if self.status() != 1:
                raise RuntimeError(
                    "DesktopLauncher detected that one or more Compose services "
                    "exited before the frontend published a Jupyter URL"
                )
            if url is not None:
                return url
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "DesktopLauncher timed out waiting for a Jupyter HTTP(S) URL"
                )
            time.sleep(self._startup_poll)

    def url(self):
        """
        Return the Jupyter URL discovered by :meth:`launch`.

        The value is ``None`` before a successful launch and after shutdown.
        No tokenless fallback URL is fabricated.
        """
        return self._url

    def launch(self):
        """
        Start or attach to the desktop stack and open its Jupyter URL.

        The method is idempotent after a successful launch.  A newly started
        project is marked as owned and is brought down if readiness or browser
        startup fails.  A fully or partially running project discovered on
        entry is caller-owned: missing services are started, but the project
        is never brought down by this object.

        :return: the HTTP(S) Jupyter URL extracted from frontend logs.
        :rtype: string
        :raises RuntimeError: if Compose execution, service readiness, URL
          discovery, or browser startup fails.
        """
        if self._url is not None:
            return self._url
        try:
            expected_services, running_services = self._service_state()
            if not expected_services.issubset(running_services):
                # Start the complete configuration.  The worker is not a
                # dependency of the frontend in the standard Compose file, so
                # targeting only the frontend would create a scheduler with no
                # worker capable of running user tasks.
                self._owns_stack = not bool(expected_services & running_services)
                self._run_compose("up", "-d")
            url = self._wait_for_url()
            try:
                self.browser_process = subprocess.Popen(self._browser_argv(url))
                browser_status = self.browser_process.poll()
            except Exception as error:
                raise RuntimeError(
                    f"DesktopLauncher failed to launch browser: {error}"
                ) from error
            if browser_status not in (None, 0):
                raise RuntimeError(
                    "DesktopLauncher browser process exited with an error"
                )
            self._url = url
            return url
        except BaseException as error:
            cleanup_errors = self._cleanup_after_failed_launch()
            if cleanup_errors and hasattr(error, "add_note"):
                error.add_note(
                    "DesktopLauncher cleanup failed: " + "; ".join(cleanup_errors)
                )
            raise

    def _service_state(self):
        """Return the configured and running service-name sets."""
        configured = self._run_compose("config", "--services")
        expected_services = {
            line.strip() for line in configured.stdout.splitlines() if line.strip()
        }
        if self._FRONTEND_SERVICE not in expected_services:
            raise RuntimeError(
                f"DesktopLauncher configuration has no {self._FRONTEND_SERVICE} service"
            )
        running = self._run_compose("ps", "--status", "running", "--services")
        running_services = {
            line.strip() for line in running.stdout.splitlines() if line.strip()
        }
        return expected_services, running_services

    def status(self):
        """
        Test readiness of the complete configured Compose project.

        Service names are read from ``docker compose config --services`` and
        compared with ``docker compose ps --status running --services``.  This
        prevents a running frontend from hiding a missing scheduler, worker,
        or database.

        :return: 1 when every configured service is running; otherwise 0.
        :rtype: int
        :raises RuntimeError: if Compose fails or the configuration has no
          ``mspass-frontend`` service.
        """
        expected_services, running_services = self._service_state()
        return int(expected_services.issubset(running_services))

    def run(self, python_file):
        """
        Run a Python script in the active frontend service.

        The command is executed as ``docker compose exec -T
        mspass-frontend python python_file`` and blocks until it exits.

        :param python_file: path to a Python script visible in the frontend.
        :type python_file: string
        :return: the successful ``subprocess.CompletedProcess``.
        :raises RuntimeError: if Compose cannot execute the command or the
          command exits nonzero.
        """
        return self._run_compose(
            "exec", "-T", self._FRONTEND_SERVICE, "python", python_file
        )

    def shutdown(self, verbose=False):
        """
        Reap the owned browser and stop an owned Compose project.

        Calling this method repeatedly is safe.  A caller-owned Compose project
        is never brought down.  Cleanup attempts continue after an individual
        failure so that all owned resources have a chance to terminate.

        :param verbose: print cleanup errors before raising them.
        :type verbose: bool
        :raises RuntimeError: after cleanup if one or more owned resources
          could not be stopped.
        """
        errors = []
        try:
            self._stop_browser()
        except BaseException as error:
            errors.append(str(error))
        if self._owns_stack:
            try:
                self._run_compose("down")
            except BaseException as error:
                errors.append(str(error))
            else:
                self._owns_stack = False
        self._url = None
        if verbose and errors:
            print("DesktopLauncher shutdown errors: " + "; ".join(errors))
        if errors:
            raise RuntimeError("DesktopLauncher shutdown failed: " + "; ".join(errors))

    def __del__(self):
        """
        Class destructor.

        The destrutor is called when an object goes out of scope.
        This instance is little more than a call to self.shutdown()
        which shuts down all the containers as gracefully as possible.
        """
        try:
            self.shutdown()
        except BaseException:
            pass


def extract_jupyter_url(outstr):
    """
    Parses output strng from launching jupyer lab to extract the url
    needed to connet to the jupyer server.

    Launchers can capture stdout from launching jupter with docker
    or aptainer and use this function to return the connection url
    to the jupyter server.

    Returns ``None`` when the output does not contain a URL.  In particular,
    this function never fabricates a tokenless fallback URL.
    """
    if not isinstance(outstr, str):
        return None
    matches = re.findall(r"https?://[^\s\"']+", outstr)
    if not matches:
        return None
    urls = [match.rstrip(".,;)") for match in matches]
    for url in urls:
        if urlsplit(url).hostname in ("127.0.0.1", "localhost", "::1"):
            return url
    return urls[0]
