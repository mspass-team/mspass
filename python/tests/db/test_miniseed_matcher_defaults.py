import inspect

import pytest

import mspasspy.db.normalize as normalize_module

BASE_ATTRIBUTES = ["starttime", "endtime", "lat", "lon", "elev", "_id"]
CHANNEL_ATTRIBUTES = BASE_ATTRIBUTES + ["hang", "vang"]


@pytest.fixture(params=["database", "cache"])
def matcher_case(request, monkeypatch):
    if request.param == "database":
        matcher_class = normalize_module.MiniseedDBMatcher
        parent_class = normalize_module.DatabaseMatcher
    else:
        matcher_class = normalize_module.MiniseedMatcher
        parent_class = normalize_module.DictionaryCacheMatcher

    captured = []

    def record_configuration(instance, *args, **kwargs):
        attributes = kwargs["attributes_to_load"]
        instance.attributes_to_load = attributes
        captured.append(attributes)

    monkeypatch.setattr(parent_class, "__init__", record_configuration)
    return matcher_class, captured


def test_default_attributes_are_fresh_and_order_independent(matcher_case):
    matcher_class, captured = matcher_case

    first_site = matcher_class(object(), collection="site")
    first_channel = matcher_class(object(), collection="channel")
    second_channel = matcher_class(object(), collection="channel")
    second_site = matcher_class(object(), collection="site")

    assert first_site.attributes_to_load == BASE_ATTRIBUTES
    assert second_site.attributes_to_load == BASE_ATTRIBUTES
    assert first_channel.attributes_to_load == CHANNEL_ATTRIBUTES
    assert second_channel.attributes_to_load == CHANNEL_ATTRIBUTES
    assert len({id(attributes) for attributes in captured}) == 4
    assert (
        inspect.signature(matcher_class).parameters["attributes_to_load"].default
        is None
    )


@pytest.mark.parametrize("input_factory", [list, tuple, iter])
def test_caller_attributes_are_copied_before_channel_additions(
    matcher_case, input_factory
):
    matcher_class, _ = matcher_case
    caller_list = list(BASE_ATTRIBUTES)
    caller_input = input_factory(caller_list)
    caller_input_snapshot = (
        list(caller_input) if isinstance(caller_input, (list, tuple)) else None
    )

    matcher = matcher_class(
        object(), collection="channel", attributes_to_load=caller_input
    )

    assert caller_list == BASE_ATTRIBUTES
    if caller_input_snapshot is not None:
        assert list(caller_input) == caller_input_snapshot
    assert matcher.attributes_to_load == CHANNEL_ATTRIBUTES
    assert matcher.attributes_to_load is not caller_input
