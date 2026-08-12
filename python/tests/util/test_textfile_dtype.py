import importlib

import pandas as pd
import pytest

from mspasspy.util import converter


def _converter(parallel):
    if parallel:
        pytest.importorskip("dask.dataframe")
        importlib.reload(converter)
        assert converter.__mspasspy_has_dask
    return converter.Textfile2Dataframe


@pytest.mark.parametrize("parallel", [False, True])
def test_textfile2dataframe_installs_requested_dtypes(tmp_path, parallel):
    convert = _converter(parallel)

    input_file = tmp_path / "typed.csv"
    input_file.write_text(
        "integer,float,text,unused\n"
        "1,1.5,10,first\n"
        "1,1.5,10,duplicate\n"
        "2,2.5,20,second\n",
        encoding="utf-8",
    )

    result = convert(
        input_file,
        separator=",",
        type_dict={"integer": "int16", "float": "float32", "text": "string"},
        attributes_to_use=["integer", "float", "text"],
        one_to_one=False,
        rename_attributes={"integer": "renamed_integer"},
        parallel=parallel,
    )

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["renamed_integer", "float", "text"]
    assert str(result["renamed_integer"].dtype) == "int16"
    assert str(result["float"].dtype) == "float32"
    assert pd.api.types.is_string_dtype(result["text"].dtype)
    assert result.to_dict("records") == [
        {"renamed_integer": 1, "float": 1.5, "text": "10"},
        {"renamed_integer": 2, "float": 2.5, "text": "20"},
    ]


@pytest.mark.parametrize("parallel", [False, True])
def test_textfile2dataframe_rejects_missing_typed_field(tmp_path, parallel):
    convert = _converter(parallel)

    input_file = tmp_path / "typed.csv"
    input_file.write_text("present\n1\n", encoding="utf-8")

    with pytest.raises(KeyError, match="missing"):
        convert(
            input_file,
            separator=",",
            type_dict={"missing": "int64"},
            attributes_to_use=["present"],
            rename_attributes={"present": "renamed"},
            parallel=parallel,
        )
