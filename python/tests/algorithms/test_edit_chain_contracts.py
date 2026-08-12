import pytest

from mspasspy.algorithms.edit import (
    Executioner,
    FiringSquad,
    MetadataOperator,
    MetadataOperatorChain,
)
from mspasspy.ccore.seismic import TimeSeries


class RecordingExecutioner(Executioner):
    def __init__(self, label, calls, result=None, arguments=None):
        self.label = label
        self.calls = calls
        self.result = result
        self.arguments = arguments

    def kill_if_true(self, datum, *args, **kwargs):
        self.calls.append(self.label)
        if self.arguments is not None:
            self.arguments.append((args, kwargs))
        if self.result is not None:
            return self.result
        return datum


class RecordingMetadataOperator(MetadataOperator):
    def __init__(self, label, calls):
        self.label = label
        self.calls = calls

    def apply(self, datum, *args, **kwargs):
        self.calls.append(self.label)
        return datum

    def check_keys(self, datum):
        return True

    def check_operation(self, datum):
        return True


def live_timeseries():
    datum = TimeSeries(1)
    datum.set_live()
    return datum


def test_executioner_callable_returns_the_named_operation_result():
    calls = []
    result = object()
    executioner = RecordingExecutioner("return", calls, result=result)
    datum = live_timeseries()

    assert executioner.kill_if_true(datum) is result
    assert executioner(datum) is result
    assert calls == ["return", "return"]


def test_executioner_callable_forwards_arguments():
    calls = []
    arguments = []
    result = object()
    executioner = RecordingExecutioner("forward", calls, result, arguments)
    datum = live_timeseries()

    assert executioner(datum, "extra", apply_to_members=True) is result
    assert calls == ["forward"]
    assert arguments == [(("extra",), {"apply_to_members": True})]


@pytest.mark.parametrize("container_type", [list, tuple])
def test_firing_squad_list_and_tuple_execute_once_in_order(container_type):
    calls = []
    executioners = [
        RecordingExecutioner("first", calls),
        RecordingExecutioner("second", calls),
    ]
    squad = FiringSquad(container_type(executioners))
    datum = live_timeseries()

    assert squad.kill_if_true(datum) is datum
    assert calls == ["first", "second"]


def test_firing_squad_materializes_a_one_shot_generator_once():
    calls = []
    yielded = []
    executioners = [
        RecordingExecutioner("first", calls),
        RecordingExecutioner("second", calls),
    ]

    def one_shot_generator():
        for executioner in executioners:
            yielded.append(executioner.label)
            yield executioner

    squad = FiringSquad(one_shot_generator())
    datum = live_timeseries()

    assert yielded == ["first", "second"]
    assert squad.kill_if_true(datum) is datum
    assert calls == ["first", "second"]


def test_firing_squad_empty_iterable_is_identity():
    squad = FiringSquad(iter(()))
    datum = live_timeseries()

    assert squad.kill_if_true(datum) is datum


@pytest.mark.parametrize("container_type", [list, tuple])
def test_metadata_operator_chain_list_and_tuple_execute_once_in_order(
    container_type,
):
    calls = []
    operators = [
        RecordingMetadataOperator("first", calls),
        RecordingMetadataOperator("second", calls),
    ]
    chain = MetadataOperatorChain(container_type(operators))
    datum = live_timeseries()

    assert chain.apply(datum) is datum
    assert calls == ["first", "second"]


def test_metadata_operator_chain_materializes_a_one_shot_generator_once():
    calls = []
    yielded = []
    operators = [
        RecordingMetadataOperator("first", calls),
        RecordingMetadataOperator("second", calls),
    ]

    def one_shot_generator():
        for operator in operators:
            yielded.append(operator.label)
            yield operator

    chain = MetadataOperatorChain(one_shot_generator())
    datum = live_timeseries()

    assert yielded == ["first", "second"]
    assert chain.apply(datum) is datum
    assert calls == ["first", "second"]


def test_metadata_operator_chain_empty_iterable_is_identity():
    chain = MetadataOperatorChain(iter(()))
    datum = live_timeseries()

    assert chain.apply(datum) is datum


@pytest.mark.parametrize(
    "chain_class,method_name,items",
    [
        (
            FiringSquad,
            "kill_if_true",
            lambda calls: [
                RecordingExecutioner("first", calls),
                RecordingExecutioner("second", calls),
            ],
        ),
        (
            MetadataOperatorChain,
            "apply",
            lambda calls: [
                RecordingMetadataOperator("first", calls),
                RecordingMetadataOperator("second", calls),
            ],
        ),
    ],
)
def test_nonempty_chain_returns_dead_input_without_running_items(
    chain_class, method_name, items
):
    calls = []
    chain = chain_class(items(calls))
    datum = live_timeseries()
    datum.kill()

    result = getattr(chain, method_name)(datum)

    assert result is datum
    assert datum.dead()
    assert calls == []


@pytest.mark.parametrize(
    "chain_class, attribute_name",
    [
        (FiringSquad, "executioners"),
        (MetadataOperatorChain, "oplist"),
    ],
)
def test_chain_constructor_rejects_noniterable_without_partial_state(
    chain_class, attribute_name
):
    candidate = chain_class.__new__(chain_class)

    with pytest.raises(TypeError):
        chain_class.__init__(candidate, 42)

    assert not hasattr(candidate, attribute_name)


@pytest.mark.parametrize(
    "chain_class, attribute_name, valid_item",
    [
        (
            FiringSquad,
            "executioners",
            lambda calls: RecordingExecutioner("valid", calls),
        ),
        (
            MetadataOperatorChain,
            "oplist",
            lambda calls: RecordingMetadataOperator("valid", calls),
        ),
    ],
)
def test_chain_constructor_rejects_invalid_second_item_before_storing_or_running(
    chain_class, attribute_name, valid_item
):
    calls = []
    candidate = chain_class.__new__(chain_class)

    with pytest.raises(TypeError):
        chain_class.__init__(candidate, [valid_item(calls), object()])

    assert not hasattr(candidate, attribute_name)
    assert calls == []


@pytest.mark.parametrize(
    "chain_class,attribute_name,valid_item",
    [
        (
            FiringSquad,
            "executioners",
            lambda label, calls: RecordingExecutioner(label, calls),
        ),
        (
            MetadataOperatorChain,
            "oplist",
            lambda label, calls: RecordingMetadataOperator(label, calls),
        ),
    ],
)
def test_invalid_second_item_is_rejected_after_materializing_entire_generator(
    chain_class, attribute_name, valid_item
):
    calls = []
    yielded = []

    def input_items():
        yielded.append("first")
        yield valid_item("first", calls)
        yielded.append("invalid")
        yield object()
        yielded.append("tail")
        yield valid_item("tail", calls)

    candidate = chain_class.__new__(chain_class)
    with pytest.raises(TypeError):
        chain_class.__init__(candidate, input_items())

    assert yielded == ["first", "invalid", "tail"]
    assert not hasattr(candidate, attribute_name)
    assert calls == []


@pytest.mark.parametrize(
    "chain_class, attribute_name, method_name, valid_item",
    [
        (
            FiringSquad,
            "executioners",
            "kill_if_true",
            lambda calls: RecordingExecutioner("valid", calls),
        ),
        (
            MetadataOperatorChain,
            "oplist",
            "apply",
            lambda calls: RecordingMetadataOperator("valid", calls),
        ),
    ],
)
def test_chain_constructor_rejects_noncallable_operation_without_partial_state(
    chain_class, attribute_name, method_name, valid_item
):
    calls = []
    invalid_item = valid_item(calls)
    setattr(invalid_item, method_name, None)
    candidate = chain_class.__new__(chain_class)

    with pytest.raises(TypeError):
        chain_class.__init__(candidate, [invalid_item])

    assert not hasattr(candidate, attribute_name)
    assert calls == []
