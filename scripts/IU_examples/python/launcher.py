#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 28 09:15:40 2025

@author: pavlis
"""
from abc import ABC, abstractmethod
import yaml   
import os
import subprocess
#import mock_subprocess as subprocess
import copy


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
        message0="HPCClusterLauncher constructor:  "
        if verbose:
            print("Loading configuration file=",configuration_file)
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
        
        # This last complex block sets hostnames that 
        # define the MsPASS frameworK;  database, scheduler, workers, and primary
        # note primary as a minimum means the host to run the python/jupyter 
        # script
        js = cluster_config["job_scheduler"]
        if js=="slurm":
            if verbose:
                print("job scheduler set as slurm")
            ph=cluster_config["primary_host"]
            dbh=cluster_config["database_host"]
            sh=cluster_config["scheduler_host"]
            wh=cluster_config["worker_hosts"]
            if (ph=="auto") or (dbh=="auto") or (sh=="auto") or (wh=="auto"):
                # this executes a slurm command to fetch nodes assigned to 
                # this job
                runline = ["scontrol","show","hostname"]
                comout = subprocess.run(runline,
                                        capture_output=True,
                                        text=True,
                                        )
                hostlist = comout.stdout.split()

                if len(hostlist)==0:
                    if self.primary_node_workers==0:
                        message = message0
                        message += "scontrol command yielded an empty list of hostnames\n"
                        message += "Cannot continue"
                        raise RuntimeError(message)
                    else:
                        comout = subprocess.run(["hostname"],
                                                 capture_output=True,
                                                 text=True)
                        hostlist=[comout.stdout]
                # comout contans a list of host names. By default for 
                # auto use the first in the list as primary
                primary=hostlist[0].strip()   # needed because of appended newline
                if ph=="auto":
                    self.primary_node=copy.deepcopy(primary)
                else:
                    self.primary_node = ph
                if dbh=="auto":
                    self.database_host=copy.deepcopy(primary)
                else:
                    self.database_host = dbh
                if sh=="auto":
                    self.scheduler_host = copy.deepcopy(primary)
                else:
                    self.scheduler_list = sh
                if wh=="auto":
                    # note worker_hoss exclude primary
                    self.worker_hosts = []
                    for i in range(1,len(hostlist),1):
                        self.worker_hosts.append(hostlist[i].strip())  # strip needed to remove newline
                            
                    if len(self.worker_hosts)<=0 and self.primary_node_workers==0:
                        message = message0
                        message += "Illegal configuration\n"
                        message += "scontrol  returned only a single hostname "
                        message += "but primary_node_workers was set to 0\n"
                        message += "To run on a single node set primary_node_workers to a postive value\n"
                        message += "To run on multiple nodes change your slurm commands at the top of this job"
                        raise RuntimeError(message)
                if verbose:
                    print("Primary node name=",self.primary_node)
                    print("database hostname=",self.database_host)
                    print("scheduler hostname=",self.scheduler_host)
                    print("Worker hostname(s)=",self.worker_hosts)
            if cluster_config["setup_tunnel"]:
                s = cluster_config["tunnel_setup_command"]
                print("Attempting to set up ssh communication tunnel to node=",
                      self.primary_node)
                print("Using this command line: {} {}".format(s,self.primary_node))
                # IMPORTANT:  actual implementation requires last arg 
                # to be primary hostname 
                arglist=s.split()
                arglist.append(self.primary_node)
                comout = subprocess.run(runline,
                                        capture_output=True,
                                        text=True)
                print("Successfully created tunnels to allow connection to ",self.primary_node)
            # these are set by the launch method but it is good practice 
            # to initialize them here
            self.scheduler_process = None
            self.dbserver_process = None
            self.primary_worker_process = None
            self.remote_worker_process = None
            self.jupyter_process=None
            if auto_launch:
                self.launch(verbose=verbose)
        else:
            message = message0
            message += "Cannot handle job_scheduler={}\n".format(js)
            message += "Currently only support slurm"
            raise ValueError(message)
    def __del__(self):
        """
        Class destructor. 
        
        The destrutor is called when an object goes out of scope. 
        This instance is little more than a call to self.shutdown()
        which shuts down all the containers as gracefully as possible.  
        """
        self.shutdown()
    def launch(self,verbose=False):
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
        """
        runline = self._initialize_container_runargs()
        runline.append(self.container_env_flag)
        envlist = "MSPASS_ROLE=scheduler,MSPASS_WORK_DIR={}".format(self.working_directory)
        envlist += ",MSPASS_SCHEDULER={}".format(self.task_scheduler)
        envlist += ",MSPASS_SCHEDULER_ADDRESS={}".format(self.primary_node)
        runline.append(envlist)
        runline.append(self.container)
        # We have to use this lower level function in subprocess 
        # for two reason:  (a) nonblocking launch to run the container 
        # from a new process and (b) keeping the output allows graceful 
        # shutdown in the shutdown method
        self.scheduler_process = subprocess.Popen(runline,
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    close_fds=True,
                                )
        if verbose:
            print("Successfully launched scheduler")
        print("Debug:  launch line")
        print(runline)
        # now do a similar thing for database 
        # note this implementation doesn't handle shrarding
        runline = self._initialize_container_runargs()
        runline.append(self.container_env_flag)
        envlist = "MSPASS_ROLE=db,"
        envlist += "MSPASS_WORK_DIR={},".format(self.working_directory)
        envlist += "MSPASS_DB_DIR={}".format(self.database_directory)
        runline.append(envlist)
        runline.append(self.container)

        self.dbserver_process = subprocess.Popen(runline,
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    close_fds=True,
                                )
        if verbose:
            print("Successfully launched db")
        print("Debug:  launch line")
        print(runline)
        # Now launch workers on hosts that are not primaary host
        worker_run_args=self._build_worker_run_args()
        if len(worker_run_args)>0:
            print("launching workers on remote hosts")
            self.remote_worker_process = subprocess.Popen(worker_run_args,
                                        stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        close_fds=True,
                                    )
        # do not trap error of no worker nodes and no workers 
        # assigned to primary - assume constructor traps that condition.
        print("Launching worker container on primary node")
        if self.primary_node_workers>0:
            # we have to launch this container differently soo 
            # we have to prepare a somewhat different run line
            # could not make this work with worker_arg unless we used 
            # shell=True.   In that case we build a command line instead of 
            # a list like runline
            runline = self._initialize_container_runargs()
            srun =""
            for s in runline:
                srun += s
                srun += " "
            srun += self.container_env_flag
            srun += " "
            envlist = "MSPASS_ROLE=worker,"
            envlist += "MSPASS_WORK_DIR={},".format(self.working_directory)
            #envlist += "MSPASS_SCHEDULER_ADDRESS={}".format(self.scheduler_host)
            envlist += "MSPASS_SCHEDULER_ADDRESS={},".format(self.scheduler_host)
            #envlist += "MSPASS_DB_ADDRESS={},".format(self.database_host)
            envlist += 'MSPASS_WORKER_ARG="--nworkers={} --nthreads 1"'.format(self.primary_node_workers)
            srun += envlist + " " + self.container
            print("command line sent to Popen")
            print(srun)
            #self.primary_worker_process = subprocess.Popen(runline,
            self.primary_worker_process = subprocess.Popen(srun,shell=True,
                                        stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        close_fds=True,
                                    )
            print("Launched workers on primary node")
            
        # Exit immmeditaly if any of the contaienrs  have exited
        if self.status(verbose=False) == 0:
            def stat_message(c):
                if self.status(container=c,verbose=False):
                    m = c + " container is running\n"
                else:
                    m = c + "container is NOT running\n"
                return m
            message = "HPCClusterLauncher:  cluster initiation failed\n"
            for con in ["db","scheduler","primary_worker"]:
                m = stat_message(con)
                message += m
            raise RuntimeError(message)

    def shutdown(self):
        # shutdown in reverse order to start
        # first workers
        # I think we only need to handle primary node container.  
        # The contaners on other nodes have to be shut down by slurm
        # unless here is an mpitrick I am unaware of
        if self.jupyter_process:
            try :
                self.jupyter_process.terminate()
                self.jupyter_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                print("Jupyter notebook server (frontend) did not respond to terminate method")
                print("Reverting to less graceful kill")
                self.jupyter_process.kill()
        if self.primary_worker_process:
            try :
                self.primary_worker_process.terminate()
                self.primary_worker_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                print("Worker container on primary node did not respond to terminate method")
                print("Reverting to less graceful kill")
                self.primary_worker_process.kill()
        # now database - should always be running so no need for not None 
        # test for the rest of these
        try :
            self.dbserver_process.terminate()
            self.dbserver_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print("Database server container did not respond to terminate method")
            print("Reverting to less graceful kill")
            self.dbserver_process.kill()
        # finally terminate the scheduler
        try :
            self.scheduler_process.terminate()
            self.scheduler_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print("Scheduler container did not respond to terminate method")
            print("Reverting to less graceful kill")
            self.scheduler_process.kill()

    def run(self,pyscript):
        """
        Runs pyscript the primary node using this cluster.
        
        This method runs a python script on the primary node. 
        It always runs in batch mode and assumes a python script 
        s the input.  We need a different method to run jupyter 
        notebooks.  Blocks until the script exits.  
        """
        # this can be made more elaborate.  Here I just run 
        # a script
        print("Trying to run python script file=",pyscript)
        runline=[]
        # I am going to hard code this for now 
        runline.append("apptainer")
        runline.append("run")
        crarg=self.container_run_args.split()
        for arg in crarg:
            runline.append(arg)
        runline.append("--env")
        envlist = "MSPASS_ROLE=frontend"
        envlist += ",MSPASS_WORK_DIR={}".format(self.working_directory)
        envlist += ",MSPASS_DB_ADDRESS={}".format(self.database_host)
        envlist += ",MSPASS_SCHEDULER_ADDRESS={}".format(self.scheduler_host)
        runline.append(envlist)
        runline.append(self.container)
        runline.append("--batch")
        runline.append(pyscript)
        print("running script")
        print("Debug:  launch line")
        print(runline)
        
        runout=subprocess.run(runline,capture_output=True,text=True)
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
        runline = self._initialize_container_runargs()
        runline.append("--env")
        envlist = "MSPASS_ROLE=frontend,"
        envlist += "MSPASS_WORK_DIR={}".format(self.working_directory)
        runline.append(envlist)
        runline.append(self.container)
        self.jupyter_process=subprocess.Popen(runline,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = self.jupyter_process.communicate()
        print(stdout)
        print(stderr)

    def status(self,container="all",verbose=True)->int:
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
                the primary node.  Note there is currently no support for 
                workers spwaned on other nodes. 
                
        Any other values for arg0 will cause this method to throw a 
        ValueError exception.
        
        :param container: container keywords noted above for arg0.  i.e. 
           must be one of "db","scheduler", "primary_worker", or "all" (default)
        :type container:  string
        :param verbose:  boolean that when True (default) uses print to 
           post a status message for container(s) requested.  When false 
           prints nothing and assumes the return will be handled
        :return:  int status.  1 means the container(s) tested were all 
           running.  0 means one or more have died.
        """
        all_containers=["db","scheduler","primary_worker"]
        if container=="all":
            statlist=all_containers
        else:
            if container in all_containers:
                statlist=[container]
            else:
                message = "HPCClusterLauncher.status:  component={}".format(container)
                message += " invalid\n"
                message += "Must be one of: "
                for c in all_containers:
                    message += c + " "
                raise ValueError(message)
        def verbose_message(container_name,poll_return):
            if poll_return is None:
                print(container_name," is running")
            else:
                print(container_name, " has exited with code=",poll_return)
        retval=1
        for container in statlist:
            match container:
                case "db":
                    stat = self.dbserver_process.poll()
                case "scheduler":
                    stat = self.scheduler_process.poll()
                case "primary_worker":
                    stat = self.primary_worker_process.poll()
            if verbose:
                verbose_message(container,stat)
            if stat:
                retval = 0
                
        return retval
                        

    def _initialize_container_runargs(self)->list:
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
        crargs=[]
        rtmp=self.container_run_command.split()
        for arg in rtmp:
            crargs.append(arg)
        rtmp=self.container_run_args.split()
        for arg in rtmp:
            crargs.append(arg)
        return crargs
    def _build_worker_run_args(self)->list:
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
        if nnodes==0:
                return []
        arglist=[]
        #cthis allows args to be entered on teh run line in config file
        tlist=self.worker_run_command.split()
        for arg in tlist:
            arglist.append(arg)
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
        for arg in self.container_run_command.split():
            arglist.append(arg)
        for arg in self.container_run_args.split():
            arglist.append(arg)
        # apptainer mthod for setting environment variables loaded 
        # in contaer
        arglist.append("--env")
        envlist = "MSPASS_ROLE=worker,"
        envlist += "MSPASS_WORK_DIR={},".format(self.working_directory)
        envlist += "MSPASS_SCHEDULER_ADDRESS={},".format(self.scheduler_host)
        envlist += "MSPASS_DB_ADDRESS={}".format(self.database_host)
        envlist += "MSPASS_WORKER_ARG=\"--nworkers={} --nthreads 1\"".format(self.workers_per_node)
        arglist.append(envlist)
        arglist.append(self.container)
        # also backgrounded 
        arglist.append("&")
        return arglist
            
            
            
    
class DesktopLauncher(BasicMsPASSLauncher):
    """
    Launcher for running MsPASS on a desktop in the all-in-one mode.
    it differs from the cluster versions in multiple ways.  First assumes 
    docker rather than apptainer.  Second, it sets the scheduler 
    as a kwarg option in the constructor rather than defining a different 
    class for different scheulers.  That is appropriate because a single 
    container makes a lot of cluster baggage unnecessary.
    """
    def __init__(self,
                 configuration="configuration_docker.yaml",
                 host_os="MacOS",
                 browser="FireFox",
                 verbose=True,
                 ):
        """
        Constructor for DesktopLauncher.
        
        This implementation uses docker compose.  The constructor does little
        more than run the docker ocmpose comand using the input configuration 
        file.
        
        :param configuration:  yaml file defining the docker compose 
          configuration to launch containers.  See User Manual section 
          title "Deply MsPASS with docker compose".
        :type configuration: string  (must be a file name ending in ".yaml" or ".yml")
        :param verbose:  boolean controling if the constructor print launch output. 
          When False runs silently unless there is an exception.  When True the 
          output of docker compose is captured and echoed to stdout of the 
          calling python script.
        """
        self.configuration_file = configuration
        runline=[]
        runline.append("docker-compose")
        runline.append("-f")
        # could test the type of configuration to be str but docker-compose 
        # will fail if this is not a valid file name
        runline.append(self.configuration_file)
        runline.append("up")
        runline.append("-d")
        runout=subprocess.run(runline,capture_output=True,text=True)
        if verbose:
            print("stdout from DecktopLauncher constructor")
            print(runout.stdout)
            print("stderr from DesktopLauncher constructor")
            print(runout.stderr)
        url = self.url()
        match host_os:
            case "MacOS":
                launch_string = "open -a "+browser + " " + url
            case "linux":
                launch_string = browser + " " + url 
            case "Window":
                raise ValueError("DesktopLauncher constructor:  windows launcing not yet implemented")

        self.browser_process = subprocess.Popen(launch_string,
                                                shell=True,
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    close_fds=True,
                                )
    def url(self)->str:
        """
        Runs docker-compos to extract url of jupyter server that is return.
        """
        runline=[]
        runline.append("docker-compose")
        runline.append("-f")
        # could test the type of configuration to be str but docker-compose 
        # will fail if this is not a valid file name
        runline.append(self.configuration_file)
        runline.append("logs")
        runline.append("mspass-frontend")
        frontend_query = subprocess.run(runline,capture_output=True,text=True)
        query_out = frontend_query.stdout
        url = extract_jupyter_url(query_out)
        return url
        
           
    def launch(self):
        print("DesktopLauncher.lauch method is not needed")
        print("constructor uses full construction is initialization concept")
    def status(self):
        # this is a p rototype command line shows mspas_fronend and 
        # mspass_db return something tut scheduler and worker 
        # return nothing - needs a different approach
        runline=[]
        runline.append("docker-compose")
        runline.append("-f")
        runline.append(self.configuration_file)
        runline.append("logs")
        runline.append("mspass_frontend")
        runout=subprocess.run(runline,capture_output=True,text=True)
        print(runout.stdout)
    def run(self):
        print("Desktop run method is not implemented for this class")
        print("Mismatch in concept as DesktopLauncher is for interactive use with jupyter")
    def shutdown(self,verbose=False):
        self.browser_process.terminate()
        runline=[]
        runline.append("docker-compose")
        runline.append("-f")
        runline.append(self.configuration_file)
        runline.append("down")
        runout=subprocess.run(runline,capture_output=True,text=True)
        if verbose:
            print("stdout from DecktopLauncher.shutdown")
            print(runout.stdout)
            print("stderr from DesktopLauncher.shutdown")
            print(runout.stderr)
    def __del__(self):
        """
        Class destructor. 
        
        The destrutor is called when an object goes out of scope. 
        This instance is little more than a call to self.shutdown()
        which shuts down all the containers as gracefully as possible.  
        """
        self.shutdown()
        

def extract_jupyter_url(outstr)->str:
    """
    Parses output strng from launching jupyer lab to extract the url 
    needed to connet to the jupyer server.
    
    Launchers can capture stdout from launching jupter with docker 
    or aptainer and use this function to return the connection url 
    to the jupyter server.  
    
    The algorithm used here is a simple search for the string "http://" 
    that the current jupyter server posts.   Output has two options and 
    the algorithm always selects the one with a ipv 4 address by veriying the 
    line has three "." characters after http://.  
    """
    test_str="http://"
    lines=outstr.splitlines()
    url_lines=[]
    for l in lines:
        if test_str in l:
            i = l.find(test_str)
            url_lines.append(l[i:])

    # select the first url with 3 or more "." symbols and assume 
    # tha is a valid ipv4 address
    for url in url_lines:
        if url.count(".")>=3:
            return url
    
    print("Error parsing jupyter server output:  returning default url with no token value")
    return "http://localhost:8888"
    
            
    
            
    
