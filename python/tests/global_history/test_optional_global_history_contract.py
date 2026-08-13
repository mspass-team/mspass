import functools
import os
import subprocess
from importlib.metadata import distribution, version
from pathlib import Path

import pytest

import mspasspy.global_history.manager as manager_module

UNSET = object()


class MapAdapter:
    def __init__(self, values):
        self.values = values

    def map(self, function, *args, **kwargs):
        return [function(value, *args, **kwargs) for value in self.values]


class ReduceAdapter:
    def __init__(self, values):
        self.values = values

    def reduce(self, function):
        return functools.reduce(function, self.values)


class FoldAdapter:
    def __init__(self, values):
        self.values = values

    def fold(self, function):
        return functools.reduce(function, self.values)


class HistorySpy:
    def __init__(self, lookup_result):
        self.lookup_result = lookup_result
        self.get_calls = []
        self.log_calls = []

    def get_alg_id(self, alg_name, parameters):
        self.get_calls.append((alg_name, parameters))
        return self.lookup_result

    def logging(self, alg_id, alg_name, parameters):
        self.log_calls.append((alg_id, alg_name, parameters))


class ObjectIdFactory:
    def __init__(self):
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return f"generated-{self.calls}"


WRAPPERS = {
    "spark_map": (manager_module.mspass_spark_map, MapAdapter, 3),
    "dask_map": (manager_module.mspass_dask_map, MapAdapter, 3),
    "spark_reduce": (manager_module.mspass_spark_reduce, ReduceAdapter, 2),
    "dask_fold": (manager_module.mspass_dask_fold, FoldAdapter, 2),
}


CASES = {
    "explicit_with_manager": {
        "alg_id": "",
        "manager_result": "unused",
        "object_history": True,
        "selected": "",
        "get_calls": 0,
        "log_calls": 1,
        "allocations": 0,
    },
    "explicit_without_manager": {
        "alg_id": "",
        "manager_result": UNSET,
        "object_history": True,
        "selected": "",
        "get_calls": 0,
        "log_calls": 0,
        "allocations": 0,
    },
    "manager_hit": {
        "alg_id": None,
        "manager_result": "registry-id",
        "object_history": True,
        "selected": "registry-id",
        "get_calls": 1,
        "log_calls": 1,
        "allocations": 0,
    },
    "manager_miss": {
        "alg_id": None,
        "manager_result": None,
        "object_history": True,
        "selected": "generated-1",
        "get_calls": 1,
        "log_calls": 1,
        "allocations": 1,
    },
    "no_manager_with_object_history": {
        "alg_id": None,
        "manager_result": UNSET,
        "object_history": True,
        "selected": "generated-1",
        "get_calls": 0,
        "log_calls": 0,
        "allocations": 1,
    },
    "no_manager_without_object_history": {
        "alg_id": None,
        "manager_result": UNSET,
        "object_history": False,
        "selected": UNSET,
        "get_calls": 0,
        "log_calls": 0,
        "allocations": 0,
    },
}


def _assert_module_from_selected_build(module, relative_path):
    source_root = os.environ.get("MSPASS_TEST_SOURCE_ROOT")
    if source_root:
        expected_module = Path(source_root) / relative_path
    else:
        expected_module = distribution("mspasspy").locate_file(relative_path)
        installed_version = version("mspasspy")
        installed_commit = installed_version.partition("+g")[2].partition(".")[0]
        assert installed_commit, "installed mspasspy version lacks a source commit"
        repository_root = next(
            parent
            for parent in Path(__file__).resolve().parents
            if (parent / ".git").exists()
        )
        checkout_commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repository_root, text=True
        ).strip()
        assert checkout_commit.startswith(installed_commit)
    assert Path(module.__file__).resolve() == Path(expected_module).resolve()


def test_contract_suite_loads_manager_from_selected_build():
    _assert_module_from_selected_build(
        manager_module, Path("mspasspy/global_history/manager.py")
    )


@pytest.mark.parametrize("wrapper_name", WRAPPERS)
@pytest.mark.parametrize("case_name", CASES)
def test_optional_global_history_matrix(monkeypatch, wrapper_name, case_name):
    wrapper, adapter_type, expected_operation_calls = WRAPPERS[wrapper_name]
    case = CASES[case_name]
    object_id_factory = ObjectIdFactory()
    monkeypatch.setattr(manager_module, "ObjectId", object_id_factory)
    history = (
        None if case["manager_result"] is UNSET else HistorySpy(case["manager_result"])
    )
    operation_calls = []

    def operation(*values, object_history=False, alg_name=None, alg_id=UNSET):
        operation_calls.append(
            {
                "object_history": object_history,
                "alg_name": alg_name,
                "alg_id": alg_id,
            }
        )
        return values[0]

    adapter = adapter_type([object(), object(), object()])
    wrapper(
        adapter,
        operation,
        global_history=history,
        object_history=case["object_history"],
        alg_id=case["alg_id"],
        alg_name="contract-operation",
        parameters="mode=contract",
    )

    assert len(operation_calls) == expected_operation_calls
    assert object_id_factory.calls == case["allocations"]
    if case["object_history"]:
        assert (
            operation_calls
            == [
                {
                    "object_history": True,
                    "alg_name": "contract-operation",
                    "alg_id": str(case["selected"]),
                }
            ]
            * expected_operation_calls
        )
    else:
        assert (
            operation_calls
            == [{"object_history": False, "alg_name": None, "alg_id": UNSET}]
            * expected_operation_calls
        )

    if history is not None:
        expected_parameters = '{"mode": "contract"}'
        assert (
            history.get_calls
            == [("contract-operation", expected_parameters)] * case["get_calls"]
        )
        assert (
            history.log_calls
            == [(case["selected"], "contract-operation", expected_parameters)]
            * case["log_calls"]
        )


@pytest.mark.parametrize("wrapper_name", ["spark_map", "dask_map"])
@pytest.mark.parametrize("alg_name", ["read_data", "save_data"])
@pytest.mark.parametrize(
    "alg_id,object_history,expected_id,allocations",
    [
        (None, True, "generated-1", 1),
        ("", True, "", 0),
        (None, False, UNSET, 0),
    ],
)
def test_map_special_paths_honor_optional_history(
    monkeypatch,
    wrapper_name,
    alg_name,
    alg_id,
    object_history,
    expected_id,
    allocations,
):
    wrapper, adapter_type, expected_operation_calls = WRAPPERS[wrapper_name]
    object_id_factory = ObjectIdFactory()
    monkeypatch.setattr(manager_module, "ObjectId", object_id_factory)
    operation_calls = []

    def operation(*values, **kwargs):
        operation_calls.append(kwargs)
        return values[0]

    adapter = adapter_type([object(), object(), object()])
    wrapper(
        adapter,
        operation,
        global_history=None,
        object_history=object_history,
        alg_id=alg_id,
        alg_name=alg_name,
        parameters="mode=contract",
    )

    assert object_id_factory.calls == allocations
    expected_kwargs = (
        {} if expected_id is UNSET else {"alg_name": alg_name, "alg_id": expected_id}
    )
    assert operation_calls == [expected_kwargs] * expected_operation_calls
