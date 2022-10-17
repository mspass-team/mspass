import copy
import os

import dask.bag as daskbag

import gridfs
import numpy as np
import obspy
import sys
import re

import pymongo
import pytest

from bson.objectid import ObjectId
from datetime import datetime

sys.path.append("python/tests")

from mspasspy.util import logging_helper
from mspasspy.db.client import DBClient
from mspasspy.db.database import Database
from helper import (
    get_live_seismogram,
    get_live_timeseries,
    get_live_timeseries_ensemble,
    get_live_seismogram_ensemble,
)
from mspasspy.global_history.manager import (
    GlobalHistoryManager,
    mspass_normal_reduce,
    mspass_normal_map,
)
import mspasspy.algorithms.signals as signals
from mspasspy.ccore.seismic import (
    Seismogram,
    TimeSeries,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
    DoubleVector,
)
from mspasspy.reduce import stack
from mspasspy.ccore.utility import Metadata, AtomicType

from mspasspy.algorithms.RFdeconProcessor import RFdeconProcessor, RFdecon
from mspasspy.ccore.utility import MsPASSError, AntelopePf
from mspasspy.util.converter import AntelopePf2dict
import json
from mspasspy.global_history.ParameterGTree import ParameterGTree, parameter_to_GTree
import collections


def spark_map(input, manager, sc, alg_name=None, parameters=None):
    data = sc.parallelize(input)
    res = data.mspass_map(
        signals.filter,
        "bandpass",
        freqmin=1,
        freqmax=5,
        object_history=True,
        global_history=manager,
        alg_name=alg_name,
        parameters=parameters,
    )
    return res.collect()


def dask_map(input, manager, alg_name=None, parameters=None):
    ddb = daskbag.from_sequence(input)
    res = ddb.mspass_map(
        signals.filter,
        "bandpass",
        freqmin=1,
        freqmax=5,
        object_history=True,
        global_history=manager,
        alg_name=alg_name,
        parameters=parameters,
    )
    return res.compute()


def dask_reduce(input, manager, alg_name=None, parameters=None):
    ddb = daskbag.from_sequence(input)
    res = ddb.mspass_reduce(
        stack,
        object_history=True,
        alg_id="3",
        global_history=manager,
        alg_name=alg_name,
        parameters=parameters,
    )
    return res.compute()


def spark_reduce(input, manager, sc, alg_name=None, parameters=None):
    data = sc.parallelize(input)
    res = data.mspass_reduce(
        stack,
        object_history=True,
        alg_id="2",
        global_history=manager,
        alg_name=alg_name,
        parameters=parameters,
    )
    return res


class TestManager:
    def setup_class(self):
        self.client = DBClient("localhost")
        self.client.drop_database("test_manager")
        db = Database(self.client, "test_manager")
        db["history_global"].drop_indexes()
        # clean up the database locally
        for col_name in db.list_collection_names():
            db[col_name].delete_many({})

        self.manager = GlobalHistoryManager(db, "test_job", collection="history_global")

    def add(
        self,
        data,
        *args,
        object_history=False,
        alg_name="filter",
        alg_id=None,
        dryrun=False,
        inplace_return=True,
        **options
    ):
        return data + data

    def test_normal_map(self):
        t = [get_live_timeseries() for i in range(5)]
        test_map_res = list(mspass_normal_map(t, self.add, global_history=self.manager))
        for i in range(5):
            assert test_map_res[i].data == t[i].data + t[i].data
        # test user provided alg_name and parameter(exist)
        alg_name = "filter"
        alg_parameters = "bandpass,freqmin=1,freqmax=5,object_history=True"
        mspass_normal_map(
            t,
            signals.filter,
            "bandpass",
            freqmin=1,
            freqmax=5,
            object_history=True,
            global_history=self.manager,
            alg_name=alg_name,
            parameters=alg_parameters,
        )

    def test_normal_reduce(self):
        t = [get_live_timeseries() for i in range(5)]
        s = t[0]
        for i in range(1, 5):
            s += t[i]
        test_reduce_res = mspass_normal_reduce(
            t, stack, global_history=self.manager, object_history=True
        )
        assert test_reduce_res.data == s.data

    def test_init(self):
        assert self.manager.job_name == "test_job"
        assert self.manager.collection == "history_global"
        assert self.manager.history_db.name == "test_manager"

    def test_logging(self):
        alg_id = ObjectId()
        manager_db = Database(self.client, "test_manager")
        manager_db["history_global"].delete_many({})
        self.manager.logging(alg_id, "test_alg_name", "test_parameter")
        res = manager_db["history_global"].find_one({"job_name": self.manager.job_name})
        assert res["job_id"] == self.manager.job_id
        assert res["job_name"] == self.manager.job_name
        assert res["alg_name"] == "test_alg_name"
        assert res["alg_id"] == alg_id
        assert res["parameters"] == "test_parameter"
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 1
        )
        # clean up
        manager_db["history_global"].delete_many({})

    def test_mspass_map(self, spark_context):
        l = [get_live_timeseries() for i in range(5)]
        # add net, sta, chan, loc to avoid metadata serialization problem
        for i in range(5):
            l[i]["chan"] = "HHZ"
            l[i]["loc"] = "test_loc"
            l[i]["net"] = "test_net"
            l[i]["sta"] = "test_sta"
            l[i].set_as_origin("test", "0", str(i), AtomicType.TIMESERIES)
        # test mspass_map for spark
        spark_res = spark_map(l, self.manager, spark_context)

        manager_db = Database(self.client, "test_manager")
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 1
        )
        res = manager_db["history_global"].find_one({"job_name": self.manager.job_name})
        assert res["job_id"] == self.manager.job_id
        assert res["job_name"] == self.manager.job_name
        assert res["alg_name"] == "filter"
        assert (
            res["parameters"]
            == '{"arg_0": "bandpass", "freqmin": "1", "freqmax": "5", "object_history": "True"}'
        )
        spark_alg_id = res["alg_id"]

        # test mspass_map for dask
        dask_res = dask_map(l, self.manager)

        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 2
        )
        assert (
            manager_db["history_global"].count_documents({"alg_id": spark_alg_id}) == 2
        )
        docs = manager_db["history_global"].find({"alg_id": spark_alg_id})
        assert docs[0]["job_id"] == docs[1]["job_id"] == self.manager.job_id
        assert docs[0]["job_name"] == docs[1]["job_name"] == self.manager.job_name
        assert docs[0]["alg_name"] == docs[1]["alg_name"] == "filter"
        assert (
            docs[0]["parameters"]
            == docs[1]["parameters"]
            == '{"arg_0": "bandpass", "freqmin": "1", "freqmax": "5", "object_history": "True"}'
        )
        assert not docs[0]["time"] == docs[1]["time"]

        # same alg + parameters combination -> same alg_id
        dask_res = dask_map(l, self.manager)
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 3
        )
        assert (
            manager_db["history_global"].count_documents({"alg_id": spark_alg_id}) == 3
        )

        # SPARK test user provided alg_name and parameter(exist)
        spark_alg_name = "filter"
        spark_alg_parameters = "bandpass,freqmin=1,freqmax=5,object_history=True"
        spark_res = spark_map(
            l,
            self.manager,
            spark_context,
            alg_name=spark_alg_name,
            parameters=spark_alg_parameters,
        )
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 4
        )
        assert (
            manager_db["history_global"].count_documents({"alg_id": spark_alg_id}) == 4
        )

        # SPARK test user provided alg_name and parameter(new)
        spark_alg_name = "new_filter"
        spark_alg_parameters = "bandpass,freqmin=1,freqmax=5,object_history=True"
        spark_res = spark_map(
            l,
            self.manager,
            spark_context,
            alg_name=spark_alg_name,
            parameters=spark_alg_parameters,
        )
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 5
        )
        assert (
            manager_db["history_global"].count_documents({"alg_name": "new_filter"})
            == 1
        )
        res = manager_db["history_global"].find_one({"alg_name": "new_filter"})
        assert res["job_id"] == self.manager.job_id
        assert res["job_name"] == self.manager.job_name
        assert res["alg_name"] == "new_filter"
        assert (
            res["parameters"]
            == '{"arg_0": "bandpass", "freqmin": "1", "freqmax": "5", "object_history": "True"}'
        )
        new_spark_alg_id = res["alg_id"]
        assert (
            manager_db["history_global"].count_documents({"alg_id": new_spark_alg_id})
            == 1
        )

        # DASK test user provided alg_name and parameter(exist)
        dask_alg_name = "filter"
        dask_alg_parameters = "bandpass,freqmin=1,freqmax=5,object_history=True"
        dask_res = dask_map(
            l, self.manager, alg_name=dask_alg_name, parameters=dask_alg_parameters
        )
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 6
        )
        assert (
            manager_db["history_global"].count_documents({"alg_id": spark_alg_id}) == 5
        )

        # DASK test user provided alg_name and parameter(new)
        dask_alg_name = "new_filter_2"
        dask_alg_parameters = "bandpass,freqmin=1,freqmax=5,object_history=True"
        dask_res = dask_map(
            l, self.manager, alg_name=dask_alg_name, parameters=dask_alg_parameters
        )
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 7
        )
        assert (
            manager_db["history_global"].count_documents({"alg_name": "new_filter_2"})
            == 1
        )
        res = manager_db["history_global"].find_one({"alg_name": "new_filter_2"})
        assert res["job_id"] == self.manager.job_id
        assert res["job_name"] == self.manager.job_name
        assert res["alg_name"] == "new_filter_2"
        assert (
            res["parameters"]
            == '{"arg_0": "bandpass", "freqmin": "1", "freqmax": "5", "object_history": "True"}'
        )
        new_dask_alg_id = res["alg_id"]
        assert (
            manager_db["history_global"].count_documents({"alg_id": new_dask_alg_id})
            == 1
        )

        manager_db["history_object"].delete_many({})
        # test spark mspass_map for save_data
        data = spark_context.parallelize(l)
        data_map = data.mspass_map(manager_db.save_data, global_history=self.manager)
        save_list = data_map.collect()
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 8
        )
        assert (
            manager_db["history_global"].count_documents({"alg_name": "save_data"}) == 1
        )
        # check object history after save_data
        manager_db["history_object"].count_documents({}) == 5
        manager_db["wf_TimeSeries"].count_documents({}) == 5
        history_object_docs = manager_db["history_object"].find({})
        idx = 0
        doc_alg_id = None
        doc_ids = []
        for doc in history_object_docs:
            if not doc_alg_id:
                doc_alg_id = doc["alg_id"]
            else:
                assert doc_alg_id == doc["alg_id"]
            doc_ids.append(doc["_id"])
            assert doc["alg_name"] == "save_data"
            idx += 1
        assert sorted(doc_ids) == ["0", "1", "2", "3", "4"]

        # test spark mspass_map for read_data
        save_l = [res[1] for res in save_list]
        data = spark_context.parallelize(save_l)
        data_map = data.mspass_map(manager_db.read_data, global_history=self.manager)
        read_list = data_map.collect()
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 9
        )
        assert (
            manager_db["history_global"].count_documents({"alg_name": "read_data"}) == 1
        )

        manager_db["history_object"].delete_many({})
        manager_db["wf_TimeSeries"].delete_many({})
        # test dask mspass_map for save_data
        data = daskbag.from_sequence(l)
        data_map = data.mspass_map(manager_db.save_data, global_history=self.manager)
        save_list = data_map.compute()
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 10
        )
        assert (
            manager_db["history_global"].count_documents({"alg_name": "save_data"}) == 2
        )
        res = manager_db["history_global"].find({"alg_name": "save_data"})
        assert res[0]["job_id"] == res[1]["job_id"] == self.manager.job_id
        assert res[0]["job_name"] == res[1]["job_name"] == self.manager.job_name
        assert res[0]["alg_name"] == res[1]["alg_name"] == "save_data"
        assert (
            res[0]["parameters"]
            == res[1]["parameters"]
            == '{"object_history": "False"}'
        )
        assert res[0]["alg_id"] == res[1]["alg_id"]
        # check object history after save_data
        manager_db["history_object"].count_documents({}) == 5
        manager_db["wf_TimeSeries"].count_documents({}) == 5
        history_object_docs = manager_db["history_object"].find({})
        idx = 0
        doc_alg_id = None
        doc_ids = []
        for doc in history_object_docs:
            if not doc_alg_id:
                doc_alg_id = doc["alg_id"]
            else:
                assert doc_alg_id == doc["alg_id"]
            doc_ids.append(doc["_id"])
            assert doc["alg_name"] == "save_data"
            idx += 1
        assert sorted(doc_ids) == ["0", "1", "2", "3", "4"]

        # test dask mspass_map for read_data
        save_l = [res[1] for res in save_list]
        data = daskbag.from_sequence(save_l)
        data_map = data.mspass_map(manager_db.read_data, global_history=self.manager)
        read_list = data_map.compute()
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 11
        )
        assert (
            manager_db["history_global"].count_documents({"alg_name": "read_data"}) == 2
        )
        res = manager_db["history_global"].find({"alg_name": "read_data"})
        assert res[0]["job_id"] == res[1]["job_id"] == self.manager.job_id
        assert res[0]["job_name"] == res[1]["job_name"] == self.manager.job_name
        assert res[0]["alg_name"] == res[1]["alg_name"] == "read_data"
        assert (
            res[0]["parameters"]
            == res[1]["parameters"]
            == '{"object_history": "False"}'
        )
        assert res[0]["alg_id"] == res[1]["alg_id"]

    def test_mspass_reduce(self, spark_context):
        manager_db = Database(self.client, "test_manager")
        manager_db["history_global"].delete_many({})

        l = [get_live_timeseries() for i in range(5)]
        # test mspass_reduce for spark
        spark_res = spark_reduce(l, self.manager, spark_context)
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 1
        )
        assert manager_db["history_global"].count_documents({"alg_name": "stack"}) == 1
        res = manager_db["history_global"].find_one({"alg_name": "stack"})
        assert res["job_id"] == self.manager.job_id
        assert res["job_name"] == self.manager.job_name
        assert res["alg_name"] == "stack"
        assert res["parameters"] == '{"object_history": "True", "alg_id": "2"}'
        spark_alg_id = res["alg_id"]

        # test mspass_reduce for dask
        dask_res = dask_reduce(l, self.manager)
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 2
        )
        assert manager_db["history_global"].count_documents({"alg_name": "stack"}) == 2
        assert (
            manager_db["history_global"].count_documents({"alg_id": spark_alg_id}) == 1
        )

        docs = manager_db["history_global"].find({"alg_name": "stack"})
        for doc in docs:
            if doc["alg_id"] == spark_alg_id:
                continue
            res = doc
        assert res["job_id"] == self.manager.job_id
        assert res["job_name"] == self.manager.job_name
        assert res["alg_name"] == "stack"
        assert res["parameters"] == '{"object_history": "True", "alg_id": "3"}'
        # different alg -> different alg_id
        assert not res["alg_id"] == spark_alg_id
        dask_alg_id = res["alg_id"]

        # same alg + parameters combination -> same alg_id
        dask_res = dask_reduce(l, self.manager)
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 3
        )
        assert manager_db["history_global"].count_documents({"alg_name": "stack"}) == 3
        assert (
            manager_db["history_global"].count_documents({"alg_id": dask_alg_id}) == 2
        )
        docs = manager_db["history_global"].find({"alg_id": dask_alg_id})
        doc1 = docs[0]
        doc2 = docs[1]
        assert not doc1["time"] == doc2["time"]
        assert doc1["job_id"] == doc2["job_id"]
        assert doc1["job_name"] == doc2["job_name"]
        assert doc1["alg_name"] == doc2["alg_name"]
        assert doc1["parameters"] == doc2["parameters"]

        # SPARK test user provided alg_name and parameter(exist)
        spark_alg_name = "stack"
        spark_alg_parameters = "object_history=True,alg_id=2"
        spark_res = spark_reduce(
            l,
            self.manager,
            spark_context,
            alg_name=spark_alg_name,
            parameters=spark_alg_parameters,
        )
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 4
        )
        assert manager_db["history_global"].count_documents({"alg_name": "stack"}) == 4
        assert (
            manager_db["history_global"].count_documents(
                {
                    "alg_name": "stack",
                    "parameters": '{"object_history": "True", "alg_id": "3"}',
                }
            )
            == 2
        )

        # SPARK test user provided alg_name and parameter(new)
        spark_alg_name = "new_stack"
        spark_alg_parameters = "object_history=True,alg_id=2"
        spark_res = spark_reduce(
            l,
            self.manager,
            spark_context,
            alg_name=spark_alg_name,
            parameters=spark_alg_parameters,
        )
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 5
        )
        assert (
            manager_db["history_global"].count_documents({"alg_name": "new_stack"}) == 1
        )
        res = manager_db["history_global"].find_one({"alg_name": "new_stack"})
        assert res["job_id"] == self.manager.job_id
        assert res["job_name"] == self.manager.job_name
        assert res["alg_name"] == "new_stack"
        assert res["parameters"] == '{"object_history": "True", "alg_id": "2"}'

        # DASK test user provided alg_name and parameter(exist)
        dask_alg_name = "stack"
        dask_alg_parameters = "object_history=True,alg_id=3"
        dask_res = dask_map(
            l, self.manager, alg_name=dask_alg_name, parameters=dask_alg_parameters
        )
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 6
        )
        assert manager_db["history_global"].count_documents({"alg_name": "stack"}) == 5
        assert (
            manager_db["history_global"].count_documents(
                {
                    "alg_name": "stack",
                    "parameters": '{"object_history": "True", "alg_id": "3"}',
                }
            )
            == 3
        )

        # DASK test user provided alg_name and parameter(new)
        dask_alg_name = "new_stack"
        dask_alg_parameters = "object_history=True,alg_id=3"
        dask_res = dask_map(
            l, self.manager, alg_name=dask_alg_name, parameters=dask_alg_parameters
        )
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 7
        )
        assert (
            manager_db["history_global"].count_documents(
                {
                    "alg_name": "new_stack",
                    "parameters": '{"object_history": "True", "alg_id": "3"}',
                }
            )
            == 1
        )
        res = manager_db["history_global"].find_one(
            {
                "alg_name": "new_stack",
                "parameters": '{"object_history": "True", "alg_id": "3"}',
            }
        )
        assert res["job_id"] == self.manager.job_id
        assert res["job_name"] == self.manager.job_name
        assert res["alg_name"] == "new_stack"
        assert res["parameters"] == '{"object_history": "True", "alg_id": "3"}'

    def test_mspass_map_with_filePath(self, spark_context):
        # test mapass_map for spark (file input)
        # data input of RFdecon, needed for parallelization
        d = [get_live_seismogram(71, 2.0) for i in range(5)]
        for i in range(5):
            d[i].t0 = -5

        # parameters string
        pfPath = "python/mspasspy/data/pf/RFdeconProcessor.pf"
        pf = AntelopePf(pfPath)
        pf_dict = AntelopePf2dict(pf)
        parameter_dict = collections.OrderedDict()
        parameter_dict["alg"] = "LeastSquares"
        parameter_dict["pf"] = pf_dict
        parameter_dict["object_history"] = "True"
        gTree = ParameterGTree(parameter_dict)
        json_params = json.dumps(gTree.asdict())

        data = spark_context.parallelize(d)
        spark_res = data.mspass_map(
            RFdecon,
            alg="LeastSquares",
            pf=pfPath,
            object_history=True,
            global_history=self.manager,
            alg_name=None,
            parameters=None,
        ).collect()
        manager_db = Database(self.client, "test_manager")
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 8
        )
        res = manager_db["history_global"].find_one({"alg_name": "RFdecon"})
        assert res["job_id"] == self.manager.job_id
        assert res["job_name"] == self.manager.job_name
        assert res["alg_name"] == "RFdecon"
        assert res["parameters"] == json_params
        spark_alg_id = res["alg_id"]

        # test mspass_map for dask
        ddb = daskbag.from_sequence(d)
        dask_res = ddb.mspass_map(
            RFdecon,
            alg="LeastSquares",
            pf=pfPath,
            object_history=True,
            global_history=self.manager,
            alg_name=None,
            parameters=None,
        ).compute()

        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 9
        )
        assert (
            manager_db["history_global"].count_documents({"alg_id": spark_alg_id}) == 2
        )
        docs = manager_db["history_global"].find({"alg_id": spark_alg_id})
        assert docs[0]["job_id"] == docs[1]["job_id"] == self.manager.job_id
        assert docs[0]["job_name"] == docs[1]["job_name"] == self.manager.job_name
        assert docs[0]["alg_name"] == docs[1]["alg_name"] == "RFdecon"
        assert docs[0]["parameters"] == docs[1]["parameters"] == json_params
        assert not docs[0]["time"] == docs[1]["time"]

        # same alg + parameters combination -> same alg_id
        ddb = daskbag.from_sequence(d)
        dask_res = ddb.mspass_map(
            RFdecon,
            alg="LeastSquares",
            pf=pfPath,
            object_history=True,
            global_history=self.manager,
            alg_name=None,
            parameters=None,
        ).compute()
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 10
        )
        assert (
            manager_db["history_global"].count_documents({"alg_id": spark_alg_id}) == 3
        )

        # SPARK test user provided alg_name and parameter(exist)
        spark_alg_name = "RFdecon"
        spark_alg_parameters = (
            "alg=LeastSquares, pf={pfPath}, object_history=True".format(pfPath=pfPath)
        )
        data = spark_context.parallelize(d)
        spark_res = data.mspass_map(
            RFdecon,
            alg="LeastSquares",
            pf=pfPath,
            object_history=True,
            global_history=self.manager,
            alg_name=spark_alg_name,
            parameters=spark_alg_parameters,
        ).collect()
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 11
        )
        assert (
            manager_db["history_global"].count_documents({"alg_id": spark_alg_id}) == 4
        )

        # SPARK test user provided alg_name and parameter(new)
        spark_alg_name = "RFdecon_2"
        spark_alg_parameters = (
            "alg=LeastSquares, pf={pfPath}, object_history=True".format(pfPath=pfPath)
        )
        data = spark_context.parallelize(d)
        spark_res = data.mspass_map(
            RFdecon,
            alg="LeastSquares",
            pf=pfPath,
            object_history=True,
            global_history=self.manager,
            alg_name=spark_alg_name,
            parameters=spark_alg_parameters,
        ).collect()
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 12
        )
        assert (
            manager_db["history_global"].count_documents({"alg_name": "RFdecon_2"}) == 1
        )
        res = manager_db["history_global"].find_one({"alg_name": "RFdecon_2"})
        assert res["job_id"] == self.manager.job_id
        assert res["job_name"] == self.manager.job_name
        assert res["alg_name"] == "RFdecon_2"
        assert res["parameters"] == json_params
        new_spark_alg_id = res["alg_id"]
        assert (
            manager_db["history_global"].count_documents({"alg_id": new_spark_alg_id})
            == 1
        )

        # DASK test user provided alg_name and parameter(exist)
        dask_alg_name = "RFdecon"
        dask_alg_parameters = (
            "alg=LeastSquares, pf={pfPath}, object_history=True".format(pfPath=pfPath)
        )
        ddb = daskbag.from_sequence(d)
        dask_res = ddb.mspass_map(
            RFdecon,
            alg="LeastSquares",
            pf=pfPath,
            object_history=True,
            global_history=self.manager,
            alg_name=dask_alg_name,
            parameters=dask_alg_parameters,
        ).compute()
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 13
        )
        assert (
            manager_db["history_global"].count_documents({"alg_id": spark_alg_id}) == 5
        )

        # DASK test user provided alg_name and parameter(new)
        dask_alg_name = "RFdecon_3"
        dask_alg_parameters = (
            "alg=LeastSquares, pf={pfPath}, object_history=True".format(pfPath=pfPath)
        )
        ddb = daskbag.from_sequence(d)
        dask_res = ddb.mspass_map(
            RFdecon,
            alg="LeastSquares",
            pf=pfPath,
            object_history=True,
            global_history=self.manager,
            alg_name=dask_alg_name,
            parameters=dask_alg_parameters,
        ).compute()
        assert (
            manager_db["history_global"].count_documents(
                {"job_name": self.manager.job_name}
            )
            == 14
        )
        assert (
            manager_db["history_global"].count_documents({"alg_name": "RFdecon_3"}) == 1
        )
        res = manager_db["history_global"].find_one({"alg_name": "RFdecon_3"})
        assert res["job_id"] == self.manager.job_id
        assert res["job_name"] == self.manager.job_name
        assert res["alg_name"] == "RFdecon_3"
        assert res["parameters"] == json_params
        new_dask_alg_id = res["alg_id"]
        assert (
            manager_db["history_global"].count_documents({"alg_id": new_dask_alg_id})
            == 1
        )

    def test_get_alg_id(self):
        manager_db = Database(self.client, "test_manager")
        assert not self.manager.get_alg_id("aaa", "bbb")
        res = manager_db["history_global"].find_one(
            {
                "alg_name": "new_stack",
                "parameters": '{"object_history": "True", "alg_id": "3"}',
            }
        )
        assert (
            self.manager.get_alg_id(
                "new_stack", '{"object_history": "True", "alg_id": "3"}'
            )
            == res["alg_id"]
        )

    def test_get_alg_list(self):
        assert (
            len(
                self.manager.get_alg_list(
                    self.manager.job_name, job_id=self.manager.job_id
                )
            )
            == 14
        )

    def test_set_alg_name_and_parameters(self):
        manager_db = Database(self.client, "test_manager")
        assert (
            manager_db["history_global"].count_documents(
                {
                    "alg_name": "stack",
                    "parameters": '{"object_history": "True", "alg_id": "3"}',
                }
            )
            == 3
        )
        res = manager_db["history_global"].find_one(
            {
                "alg_name": "stack",
                "parameters": '{"object_history": "True", "alg_id": "3"}',
            }
        )
        alg_id = res["alg_id"]
        self.manager.set_alg_name_and_parameters(
            alg_id, "test_alg_name", "test_parameters"
        )
        assert (
            manager_db["history_global"].count_documents(
                {
                    "alg_name": "stack",
                    "parameters": '{"object_history": "True", "alg_id": "3"}',
                }
            )
            == 0
        )
        assert (
            manager_db["history_global"].count_documents(
                {"alg_name": "test_alg_name", "parameters": "test_parameters"}
            )
            == 3
        )
        res = manager_db["history_global"].find_one(
            {"alg_name": "test_alg_name", "parameters": "test_parameters"}
        )
        assert res["alg_id"] == alg_id

    def test_object_history(self, spark_context):
        manager_db = Database(self.client, "test_manager")
        manager_db["history_global"].delete_many({})
        manager_db["history_object"].delete_many({})
        l = [get_live_timeseries() for i in range(2)]
        # add net, sta, chan, loc to avoid metadata serialization problem
        for i in range(2):
            l[i]["chan"] = "HHZ"
            l[i]["loc"] = "test_loc"
            l[i]["net"] = "test_net"
            l[i]["sta"] = "test_sta"
        spark_res = spark_map(l, self.manager, spark_context)
        assert manager_db["history_global"].count_documents({"alg_name": "filter"}) == 1
        res = manager_db["history_global"].find_one({"alg_name": "filter"})
        alg_id = res["alg_id"]
        # check status of the mspass objects
        for ts in spark_res:
            assert ts.number_of_stages() == 1
            assert ts.current_nodedata().algorithm == "filter"
            assert ts.current_nodedata().algid == str(alg_id)
            assert ts.is_volatile()

        save_res = manager_db.save_data(
            spark_res[0], alg_name="filter", alg_id=str(alg_id)
        )
        # hardcode net, sta, net, loc to avoid serialization problem here, they are readonly metadata keys -> non fatal keys = 4
        assert save_res.live
        assert manager_db["history_object"].count_documents({"alg_name": "filter"}) == 1
        doc = manager_db["history_object"].find_one({"alg_name": "filter"})
        assert doc
        assert doc["_id"] == spark_res[0].current_nodedata().uuid
        assert doc["wf_TimeSeries_id"] == spark_res[0]["_id"]
        assert doc["alg_id"] == str(alg_id)
        assert doc["alg_name"] == "filter"
