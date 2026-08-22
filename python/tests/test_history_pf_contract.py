from collections import OrderedDict
from copy import deepcopy

import pytest

from mspasspy.ccore.utility import AntelopePf
from mspasspy.ccore.utility import MsPASSError
from mspasspy.history import pf_history_data, pfbranch_to_dict
from mspasspy.util.converter import AntelopePf2dict

PF_TEXT = """
root_value root
root_integer 7
root_table &Tbl{
root row one
root row two
}
selected &Arr{
    selected_value level_one
    internal_real 1.5
    internal_table &Tbl{
internal row one
internal row two
    }
    middle &Arr{
        middle_value level_two
        middle_table &Tbl{
middle row
        }
        leaf &Arr{
            leaf_value level_three
            leaf_flag true
            leaf_table &Tbl{
leaf row one
leaf row two
            }
        }
    }
    selected_sibling &Arr{
        selected_sibling_value retained
    }
}
unselected &Arr{
    unselected_value do_not_leak
    unselected_table &Tbl{
unrelated row
    }
}
"""


SELECTED = {
    "selected_value": "level_one",
    "internal_real": 1.5,
    "internal_table": ["internal row one", "internal row two"],
    "middle": {
        "middle_value": "level_two",
        "middle_table": ["middle row"],
        "leaf": {
            "leaf_value": "level_three",
            "leaf_flag": True,
            "leaf_table": ["leaf row one", "leaf row two"],
        },
    },
    "selected_sibling": {"selected_sibling_value": "retained"},
}


def _pf(tmp_path):
    path = tmp_path / "nested_history.pf"
    path.write_text(PF_TEXT)
    return AntelopePf(str(path))


def test_pfbranch_to_dict_decodes_only_the_requested_branch(tmp_path):
    pf = _pf(tmp_path)
    before = deepcopy(AntelopePf2dict(pf))

    result = pfbranch_to_dict(pf, "selected")

    assert result == SELECTED
    assert type(result) is OrderedDict
    assert type(result["middle"]) is OrderedDict
    assert type(result["middle"]["leaf"]) is OrderedDict
    assert type(result["internal_real"]) is float
    assert type(result["middle"]["leaf"]["leaf_flag"]) is bool
    result_keys = list(result)
    assert set(result_keys[:2]) == {"selected_value", "internal_real"}
    assert result_keys[2:] == ["internal_table", "middle", "selected_sibling"]
    assert "unselected" not in result
    result["internal_table"].append("returned-value mutation")
    result["middle"]["leaf"]["leaf_table"][0] = "returned-value mutation"
    assert AntelopePf2dict(pf) == before


def test_pfbranch_to_dict_missing_branch_preserves_pf_and_raises(tmp_path):
    pf = _pf(tmp_path)
    before = deepcopy(AntelopePf2dict(pf))

    with pytest.raises(MsPASSError, match="missing"):
        pfbranch_to_dict(pf, "missing")

    assert AntelopePf2dict(pf) == before


def test_pf_history_data_preserves_simple_tbl_and_nested_arr_values(tmp_path):
    pf = _pf(tmp_path)
    before = deepcopy(AntelopePf2dict(pf))

    history = pf_history_data(17, "nested_algorithm", pf)

    assert history.jobid == 17
    assert history.algorithm == "nested_algorithm"
    assert history.param_type == "AntelopePf"
    assert type(history.params) is OrderedDict
    assert type(history.params["selected"]) is OrderedDict
    assert type(history.params["selected"]["middle"]["leaf"]) is OrderedDict
    assert type(history.params["root_integer"]) is int
    assert history.params == {
        "root_value": "root",
        "root_integer": 7,
        "root_table": ["root row one", "root row two"],
        "selected": SELECTED,
        "unselected": {
            "unselected_value": "do_not_leak",
            "unselected_table": ["unrelated row"],
        },
    }
    assert AntelopePf2dict(pf) == before
