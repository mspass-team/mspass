.. _io:

I/O in MsPASS
========================================

Overview
-----------------------

We want to use parallel containers to load data for efficiency, and the process of reading 
data includes reading from the file system and reading from the database. To further improve 
efficiency, we want to avoid database related operations and only perform file system read and write 
operations in the parallel containers. This module describes distributed I/O operations in 
MsPASS without using Database. The functions here are not methods of Database.

Read
~~~~~~~
Read is the proccess of loading the entire data set into a parallel container (RDD for SPARK 
implementations or BAG for a DASK implementations). 

1.  :py:meth:`read_distributed_data <mspasspy.io.distributed.read_distributed_data>` is the core method for reading data.

The input includes the metadata information stored in the dataframe. Depending on the storage mode, 
the metadata should include parameters such as file path, gridfs id, etc. This function will 
construct the objects from storage using DASK or SPARK, and loading complete mspass objects into 
the parallel container. A block of example code should make this clearer:

    .. code-block:: python

        from mspasspy.db.client import Client
        from mspasspy.db.database import Database
        from mspasspy.io.distributed import read_distributed_data
        dbclient = Client()
        # This is the name used to acccess the database of interest assumed
        # to contain data loaded previously.  Name used would change for user
        dbname = 'distributed_data_example'  # name of db set in MongoDB - example
        db = Database(dbclient,dbname)
        # This example reads all the data currently stored in this database
        cursor = db.wf_TimeSeries.find({})
        df = read_to_dataframe(dbclient, cursor) # store to dataframe
        rdd0 = read_distributed_data(
            df,
            cursor=None,
            format="spark",
            mode="promiscuous",
            spark_context=spark_context,
        )

    The output of the read is the SPARK RDD that we assign the symbol rdd0.
    If you are using DASK instead of SPARK you would add the optional
    argument :code:`format='dask'`.


Write
~~~~~~~
Write is the proccess of saving the entire data set into the file system.

1.  :py:meth:`write_distributed_data <mspasspy.io.distributed.write_distributed_data>` is the core method for writing data.

The input includes SPARK RDD or DASK BAG of objects (TimeSeries or Seismogram), and the output is a dataframe of metadata. 
From the container, it will firstly write to files distributedly using SPARK or DASK, and then write to the database 
sequentially. It returns a dataframe of metadata for each object in the original container. The return value 
can be used as input for :code:`read_distributed_data` function. 

Note that the objects should be written to different files, otherwise it may overwrite each other. 
dir and dfile should be stored in each object.

A block of example code should make this clearer:

    .. code-block:: python

        import dask.bag as daskbag
        ts_list = [ts1, ts2, ts3] # given TimeSeries list
        list_ = daskbag.from_sequence(ts_list)
        # write to file and get a dataframe
        df = write_distributed_data(
            list_,
            db,
            mode="cautious",
            storage_mode="file",
            format="dask",
        ) 
        # read from the dataframe
        obj_list = read_distributed_data(df, format="dask").compute()
        

    The output of the write is the dataframe containing the metadata information that 
    can be used in :code:`read_distributed_data` function.
