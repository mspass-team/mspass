import pytest
from mspasspy.global_history.ParameterGTree import ParameterGTree, parameter_to_GTree
from mspasspy.util.converter import AntelopePf2dict
from mspasspy.ccore.utility import MsPASSError, AntelopePf
import json
import os
import yaml
import collections
import copy
from mspasspy.ccore.utility import MsPASSError, AntelopePf
from mspasspy.util.converter import AntelopePf2dict


def build_helper():
    #   A helper function to build a GTree instance for test.
    pfPath = "./data/pf/GTreeTest.pf"
    pf = AntelopePf(pfPath)
    pf_dict = AntelopePf2dict(pf)
    parameter_dict = collections.OrderedDict()
    parameter_dict["alg"] = "LeastSquares"
    parameter_dict["pf"] = pf_dict
    parameter_dict["object_history"] = "True"
    gTree = ParameterGTree(parameter_dict)
    return gTree


def leaf_helper():
    return ("alg", "LeastSquares")


def branch_helper():
    pfPath = "./data/pf/GTreeTest.pf"
    pf = AntelopePf(pfPath)
    pf_dict = collections.OrderedDict(AntelopePf2dict(pf))
    return ("pf", pf_dict)


def test_tree_to_dict():
    json_tree = '{"alg": "LeastSquares", "pf": {"LeastSquare": {"deconvolution_data_window_end": 30.0, "shaping_wavelet_frequency_for_inverse": 0.5, "operator_nfft": 1024, "shaping_wavelet_dt": 0.05, "deconvolution_data_window_start": -5.0, "target_sample_interval": 0.05, "damping_factor": 1.0, "shaping_wavelet_frequency": 1.0, "shaping_wavelet_type": "ricker"}, "MultiTaperSpecDiv": {"noise_window_start": -35.0, "deconvolution_data_window_end": 30.0, "shaping_wavelet_frequency_for_inverse": 0.5, "operator_nfft": 1024, "shaping_wavelet_dt": 0.05, "deconvolution_data_window_start": -5.0, "noise_window_end": -5.0, "time_bandwidth_product": 2.5, "number_tapers": 4, "target_sample_interval": 0.05, "damping_factor": 0.1, "shaping_wavelet_frequency": 1.0, "shaping_wavelet_type": "ricker"}, "MultiTaperXcor": {"noise_window_start": -35.0, "deconvolution_data_window_end": 30.0, "shaping_wavelet_frequency_for_inverse": 0.5, "operator_nfft": 1024, "shaping_wavelet_dt": 0.05, "deconvolution_data_window_start": -5.0, "noise_window_end": -5.0, "time_bandwidth_product": 2.5, "number_tapers": 4, "target_sample_interval": 0.05, "damping_factor": 0.05, "shaping_wavelet_frequency": 1.0, "shaping_wavelet_type": "ricker"}, "WaterLevel": {"deconvolution_data_window_end": 30.0, "shaping_wavelet_frequency_for_inverse": 1.0, "operator_nfft": 1024, "shaping_wavelet_dt": 0.05, "deconvolution_data_window_start": -5.0, "water_level": 1.0, "target_sample_interval": 0.05, "shaping_wavelet_frequency": 1.0, "shaping_wavelet_type": "ricker"}}, "object_history": "True"}'
    gTree = build_helper()
    json_dump = json.dumps(gTree.asdict())
    json_dump_native = json.dumps(gTree)
    assert json_dump == json_dump_native
    assert json.loads(json_dump) == json.loads(json_tree)


def test_get_leaf_keys():
    gTree = build_helper()
    keys = gTree.get_leaf_keys()
    assert keys == ["alg", "object_history"]


def test_get_branch_keys():
    gTree = build_helper()
    keys = gTree.get_branch_keys()
    assert keys == ["pf"]


def build_prune_tree():
    return ParameterGTree(
        collections.OrderedDict(
            [
                ("first_leaf", ["first"]),
                (
                    "middle_branch",
                    collections.OrderedDict(
                        [
                            ("nested_leaf", ["nested"]),
                            (
                                "nested_branch",
                                collections.OrderedDict([("deep_leaf", "deep")]),
                            ),
                        ]
                    ),
                ),
                ("middle_leaf", ["middle"]),
                (
                    "final_branch",
                    collections.OrderedDict([("final_leaf", ["final"])]),
                ),
            ]
        )
    )


@pytest.mark.parametrize(
    "key", ["first_leaf", "middle_branch", "middle_leaf", "final_branch"]
)
def test_prune_removes_only_requested_key(key):
    gTree = build_prune_tree()
    before = copy.deepcopy(gTree.asdict())
    expected_tree = copy.deepcopy(before)
    del expected_tree[key]

    pruned_value = gTree.prune(key)

    assert pruned_value == before[key]
    assert gTree.asdict() == expected_tree


def test_prune_nested_leaf_preserves_full_tree():
    gTree = build_prune_tree()
    before = copy.deepcopy(gTree.asdict())
    expected_tree = copy.deepcopy(before)
    del expected_tree["middle_branch"]["nested_leaf"]

    pruned_value = gTree["middle_branch"].prune("nested_leaf")

    assert pruned_value == before["middle_branch"]["nested_leaf"]
    assert gTree.asdict() == expected_tree


def test_prune_missing_key_preserves_full_tree():
    gTree = build_prune_tree()
    before = copy.deepcopy(gTree.asdict())

    with pytest.raises(MsPASSError, match="Wrong Key"):
        gTree.prune("missing")

    assert gTree.asdict() == before


def test_get_leaf():
    gTree = build_helper()
    leaf_key, leaf_val = leaf_helper()
    assert gTree.get_leaf(leaf_key) == leaf_val
    assert gTree[leaf_key] == leaf_val


def test_get_branch():
    gTree = build_helper()
    branch_key, branch_val = branch_helper()
    assert gTree.get_branch(branch_key) == branch_val
    assert gTree[branch_key] == branch_val


def test_put():
    #   Putting a leaf
    gTree = ParameterGTree()
    leaf_key, leaf_val = leaf_helper()
    gTree.put(leaf_key, leaf_val)
    assert leaf_key in gTree.get_leaf_keys()
    assert gTree[leaf_key] == leaf_val
    gTree.put("new_temp_level." + leaf_key, leaf_val)
    assert "new_temp_level" in gTree.get_branch_keys()
    assert leaf_key in gTree["new_temp_level"].get_leaf_keys()
    assert gTree["new_temp_level"].get_leaf(leaf_key) == leaf_val

    #   Putting a branch
    branch_key, branch_val = branch_helper()
    gTree[branch_key] = branch_val
    assert branch_key in gTree.get_branch_keys()
    assert gTree[branch_key] == branch_val

    gTree["new_temp_level"] = ParameterGTree()
    gTree["new_temp_level"][branch_key] = branch_val
    assert "new_temp_level" in gTree.get_branch_keys()
    assert branch_key in gTree["new_temp_level"].get_branch_keys()
    assert gTree["new_temp_level"].get_branch(branch_key) == branch_val


def test_get():
    gTree = build_helper()
    #   Test to get a leaf
    alg_val = gTree.get("alg")
    alg_val_by_index = gTree["alg"]
    damping_factor_val = gTree.get("pf.LeastSquare.damping_factor")
    damping_factor_val_by_index = gTree["pf"]["LeastSquare"]["damping_factor"]

    assert alg_val == "LeastSquares"
    assert alg_val_by_index == "LeastSquares"
    assert damping_factor_val == 1.0
    assert damping_factor_val_by_index == 1.0

    #   Test to get a branch
    branch_key, branch_val = branch_helper()
    pf_val_by_index = gTree[branch_key]
    assert pf_val_by_index == branch_val
