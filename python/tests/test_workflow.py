from collections import Counter
from contextlib import contextmanager
import gc
from pathlib import Path
import subprocess
import sys
import threading
import time
import weakref

import dask
import pytest
import dask.distributed as ddist
import numpy as np
from mspasspy.workflow import sliding_window_pipeline


# run functions used for tests
def simple_no_args(a):
    # ddist.print("Running a=",a)
    b = a + 1
    return b


def simple_warg(a, b):
    # ddist.print(f"Running with {a=} and {b=}")
    x = a + b
    return x


def simple_wkwarg(a, b=2):
    # ddist.print(f"Running with {a=} and {b=}")
    x = a + b
    return x


def simple_full(a, b, c, d=5):
    # ddist.print(f"Running with {a=}, {b=}, {c=}")
    # ddist.print("Running with kwarg value {d=}")
    x = a + b + c + d
    return x


def simple_completion(a):
    return a + 1


def completion_full(a, b, c=10):
    ddist.print(a, b, c)
    return a + b + c


def simple_accumulator(old, x):
    print(f"{old=} {x=}")
    if old is None:
        return x
    else:
        return old + x


def accumulator_full(old, x, a, b=30):
    print(f"{old=} {x=} {a=} {b=}")
    if old is None:
        return x + a + b
    else:
        print("accmulator_full return=", old + x + a + b)
        return old + x + a + b


def delayed_value(item):
    value, delay = item
    time.sleep(delay)
    return value


def times_ten(value):
    return value * 10


def append_accumulator(old, value):
    if old is None:
        return [value]
    return old + [value]


def fail_or_delay(item):
    should_fail, delay = item
    time.sleep(delay)
    if should_fail:
        raise RuntimeError("processing failed")
    return should_fail


def fail_completion(value):
    if value == "fail":
        raise RuntimeError("completion failed")
    return value


def fail_accumulator(old, value):
    raise RuntimeError("accumulator failed")


class CompletionPayload:
    pass


class WorkerOnlyPayload:
    def __reduce__(self):
        raise RuntimeError("worker-only payload was serialized")


def make_worker_only_payload(_):
    return WorkerOnlyPayload()


def return_input(payload):
    return payload


def report_completion_location(value, prefix="", suffix=""):
    try:
        ddist.get_worker()
    except ValueError:
        location = "driver"
    else:
        location = "worker"
    return f"{prefix}{location}{suffix}", value


def make_or_block_payload(item, second_task_started, release_second_task):
    if item == 1:
        second_task_started.set()
        if not release_second_task.wait(timeout=5):
            raise TimeoutError("timed out waiting to release second task")
    return CompletionPayload()


def record_and_return_payload(payload, references):
    references.append(weakref.ref(payload))
    return payload


class OneShotIterable:
    def __init__(self, values):
        self.values = values
        self.iterations = 0

    def __iter__(self):
        self.iterations += 1
        if self.iterations > 1:
            raise AssertionError("iterable was consumed more than once")
        return iter(self.values)


@contextmanager
def _local_dask_client():
    with ddist.LocalCluster(
        n_workers=1,
        threads_per_worker=4,
        processes=False,
        dashboard_address=None,
    ) as cluster:
        with ddist.Client(cluster) as client:
            yield client


@pytest.fixture
def dask_client():
    with _local_dask_client() as client:
        yield client


@pytest.fixture
def process_dask_client():
    with ddist.LocalCluster(
        n_workers=1,
        threads_per_worker=1,
        processes=True,
        dashboard_address=None,
    ) as cluster:
        with ddist.Client(cluster) as client:
            yield client


def test_import_does_not_install_a_default_dask_client():
    repository_root = Path(__file__).resolve().parents[2]
    script = """
import runpy
from dask.distributed import default_client

runpy.run_path("python/tests/test_workflow.py")
try:
    default_client()
except ValueError:
    pass
else:
    raise AssertionError("importing test_workflow installed a default Dask client")
"""
    subprocess.run([sys.executable, "-c", script], cwd=repository_root, check=True)


def test_local_dask_client_cleanup_preserves_unrelated_compute():
    assert dask.delayed(simple_no_args)(0).compute() == 1
    with _local_dask_client() as client:
        assert client.submit(simple_no_args, 1).result() == 2
    assert dask.delayed(simple_no_args)(2).compute() == 3


def test_pipeline_uses_the_supplied_non_default_client(dask_client, monkeypatch):
    observed_loops = []
    as_completed = ddist.as_completed

    def record_loop(*args, **kwargs):
        observed_loops.append(kwargs.get("loop"))
        return as_completed(*args, **kwargs)

    monkeypatch.setattr(ddist, "as_completed", record_loop)
    with ddist.LocalCluster(
        n_workers=1,
        threads_per_worker=1,
        processes=False,
        dashboard_address=None,
    ) as cluster:
        with ddist.Client(cluster, set_as_default=False) as supplied_client:
            assert ddist.default_client() is dask_client
            assert supplied_client is not dask_client
            assert sliding_window_pipeline(
                [1],
                simple_no_args,
                supplied_client,
                sliding_window_size=1,
            ) == [2]
            assert observed_loops == [supplied_client.loop]


def test_sliding_window_pipeline(dask_client, capsys):
    """
    Test function for `sliding_window_pipeline` function.

    It uses a vector of integers to run through a processing function using functions
    defined above.  It tests proper handling of args and kwargs for the processing function.
    It run runs similar tests for the completion function and accumulator.
    All but the accumulator tests use a vector random integers to make a "in"
    test definitive.  The test outputs, however, do not depend upon the actual
    value of the integers used only the operations applied to them.

    The test for the accumulator function are a little different because it has
    to handle sums of data passed through the processing and completion
    functions.  For that reason those tests use a sequence of integers
    that will always yield the same sums.
    """
    listsize = 10
    # use random integers so tests are unlikely to randomly work or fail
    # necessary as the processing functions are all trivial integer arithmetic operations
    dlist = np.random.randint(1, 5000, size=listsize)
    result = sliding_window_pipeline(
        dlist, simple_no_args, dask_client, sliding_window_size=4
    )
    # print(result)
    expected_out = (
        dlist + 1
    )  # numpy vector overload makes this a simple way to create this

    assert Counter(result) == Counter(expected_out.tolist())
    # repeat with verbose on and sliding_window_size set auto
    result = sliding_window_pipeline(
        dlist,
        simple_no_args,
        dask_client,
        sliding_window_size="auto",
        verbose=True,
        progress_report_interval=3,
    )
    assert Counter(result) == Counter(expected_out.tolist())
    progress = capsys.readouterr().out
    for handled in (3, 6, 9, 10):
        assert f"Handled {handled} of 10 items" in progress
    assert "Submitting item" not in progress

    # run function with an arg to pfunc_args
    result = sliding_window_pipeline(
        dlist, simple_warg, dask_client, sliding_window_size=4, pfunc_args=[2]
    )
    expected_out = dlist + 2
    assert Counter(result) == Counter(expected_out.tolist())

    # same with pfun_kwarg
    kwa = {"b": 3}
    result = sliding_window_pipeline(
        dlist, simple_wkwarg, dask_client, sliding_window_size=4, pfunc_kwargs=kwa
    )
    expected_out = dlist + 3
    assert Counter(result) == Counter(expected_out.tolist())

    # similar but let kwargs default
    result = sliding_window_pipeline(
        dlist, simple_wkwarg, dask_client, sliding_window_size=4
    )
    expected_out = dlist + 2  # note 2 must match default of function
    assert Counter(result) == Counter(expected_out.tolist())

    # run function with multiple args
    kwa = {"d": 4}
    result = sliding_window_pipeline(
        dlist,
        simple_full,
        dask_client,
        sliding_window_size=4,
        pfunc_args=[1, 2],
        pfunc_kwargs=kwa,
    )
    expected_out = dlist + 7  # sum of all 3 args passed
    assert Counter(result) == Counter(expected_out.tolist())

    # run same with a completion that just adds 1 with no args
    result = sliding_window_pipeline(
        dlist,
        simple_full,
        dask_client,
        sliding_window_size=4,
        pfunc_args=[1, 2],
        pfunc_kwargs=kwa,
        completion_function=simple_completion,
    )
    expected_out = dlist + 7 + 1  # sum of all 3 args passed + completion add 1
    assert Counter(result) == Counter(expected_out.tolist())

    # now run with arg and default kwarg
    result = sliding_window_pipeline(
        dlist,
        simple_full,
        dask_client,
        sliding_window_size=4,
        pfunc_args=[1, 2],
        pfunc_kwargs=kwa,
        completion_function=completion_full,
        cfunc_args=[20],
    )
    addamount = 7 + 20 + 10
    expected_out = dlist + addamount
    assert Counter(result) == Counter(expected_out.tolist())

    # now add cfunc kwarg
    result = sliding_window_pipeline(
        dlist,
        simple_full,
        dask_client,
        sliding_window_size=4,
        pfunc_args=[1, 2],
        pfunc_kwargs=kwa,
        completion_function=completion_full,
        cfunc_args=[20],
        cfunc_kwargs={"c": 30},
    )
    addamount = 7 + 20 + 30
    expected_out = dlist + addamount
    assert Counter(result) == Counter(expected_out.tolist())

    # finally test accmulator feature
    # change the input in this case as all we care about this the summed output
    for i in range(len(dlist)):
        dlist[i] = i
    print("Testing simple_accumulator")
    result = sliding_window_pipeline(
        dlist,
        simple_full,
        dask_client,
        sliding_window_size=4,
        pfunc_args=[1, 2],
        pfunc_kwargs={"d": 10},
        completion_function=simple_completion,
        accumulator=simple_accumulator,
    )
    expected_result = 0
    for i in range(len(dlist)):
        expected_result = expected_result + dlist[i] + 1 + 2 + 10 + 1
    assert result == expected_result

    # add a_args
    print("testing accumulator_full with arg set and default kwarg")
    result = sliding_window_pipeline(
        dlist,
        simple_full,
        dask_client,
        sliding_window_size=4,
        pfunc_args=[1, 2],
        pfunc_kwargs={"d": 10},
        completion_function=simple_completion,
        accumulator=accumulator_full,
        a_args=[5],
    )
    expected_result = 0
    for i in range(len(dlist)):
        expected_result = expected_result + dlist[i] + 1 + 2 + 10 + 1 + 5 + 30
    assert result == expected_result

    # add a_kwargs
    result = sliding_window_pipeline(
        dlist,
        simple_full,
        dask_client,
        sliding_window_size=4,
        pfunc_args=[1, 2],
        pfunc_kwargs={"d": 10},
        completion_function=simple_completion,
        accumulator=accumulator_full,
        a_args=[5],
        a_kwargs={"b": 42},
    )
    expected_result = 0
    for i in range(len(dlist)):
        expected_result = expected_result + dlist[i] + 1 + 2 + 10 + 1 + 5 + 42
    assert result == expected_result


def test_completion_order_and_return_modes(dask_client):
    items = [(0, 0.3), (1, 0.15), (2, 0.01)]

    result = sliding_window_pipeline(
        items, delayed_value, dask_client, sliding_window_size=3
    )
    assert result == [2, 1, 0]

    result = sliding_window_pipeline(
        items,
        delayed_value,
        dask_client,
        sliding_window_size=3,
        completion_function=times_ten,
    )
    assert result == [20, 10, 0]

    result = sliding_window_pipeline(
        items,
        delayed_value,
        dask_client,
        sliding_window_size=3,
        completion_function=times_ten,
        accumulator=append_accumulator,
    )
    assert result == [20, 10, 0]


def test_results_can_be_discarded(dask_client):
    result = sliding_window_pipeline(
        [1, 2, 3],
        simple_no_args,
        dask_client,
        sliding_window_size=1,
        completion_function=simple_completion,
        retain_results=False,
    )

    assert result is None

    result = sliding_window_pipeline(
        [1, 2, 3],
        simple_no_args,
        dask_client,
        sliding_window_size=1,
        retain_results=False,
    )
    assert result is None


def test_discarded_processing_result_is_not_gathered(process_dask_client):
    result = sliding_window_pipeline(
        [1],
        make_worker_only_payload,
        process_dask_client,
        sliding_window_size=1,
        retain_results=False,
    )

    assert result is None


def test_completion_can_run_and_discard_output_on_worker(process_dask_client):
    result = sliding_window_pipeline(
        [1],
        make_worker_only_payload,
        process_dask_client,
        sliding_window_size=1,
        completion_function=return_input,
        retain_results=False,
        completion_on_worker=True,
    )

    assert result is None


def test_completion_location_and_worker_result_modes(dask_client):
    driver_result = sliding_window_pipeline(
        [1],
        simple_no_args,
        dask_client,
        sliding_window_size=1,
        completion_function=report_completion_location,
    )
    worker_result = sliding_window_pipeline(
        [1],
        simple_no_args,
        dask_client,
        sliding_window_size=1,
        completion_function=report_completion_location,
        cfunc_args=["on-"],
        cfunc_kwargs={"suffix": "-side"},
        completion_on_worker=True,
    )
    accumulated_result = sliding_window_pipeline(
        [1, 2],
        simple_no_args,
        dask_client,
        sliding_window_size=1,
        completion_function=times_ten,
        accumulator=simple_accumulator,
        retain_results=False,
        completion_on_worker=True,
    )

    assert driver_result == [("driver", 2)]
    assert worker_result == [("on-worker-side", 2)]
    assert accumulated_result == 50


def test_future_cancel_is_requested_before_driver_completion(dask_client, monkeypatch):
    submitted = []
    statuses_during_completion = []
    original_submit = dask_client.submit

    def capture_submit(*args, **kwargs):
        future = original_submit(*args, **kwargs)
        submitted.append(future)
        return future

    def inspect_future_status(value):
        statuses_during_completion.append(submitted[0].status)
        return value

    monkeypatch.setattr(dask_client, "submit", capture_submit)

    result = sliding_window_pipeline(
        [1],
        simple_no_args,
        dask_client,
        sliding_window_size=1,
        completion_function=inspect_future_status,
    )

    assert result == [2]
    assert statuses_during_completion == ["cancelled"]


def test_discarded_result_is_released_while_pipeline_runs(dask_client):
    second_task_started = ddist.Event()
    release_second_task = ddist.Event()
    references = []
    errors = []

    def run_pipeline():
        try:
            sliding_window_pipeline(
                [0, 1],
                make_or_block_payload,
                dask_client,
                sliding_window_size=1,
                completion_function=record_and_return_payload,
                pfunc_args=[second_task_started, release_second_task],
                cfunc_args=[references],
                retain_results=False,
            )
        except BaseException as error:
            errors.append(error)

    pipeline_thread = threading.Thread(target=run_pipeline)
    pipeline_thread.start()
    second_started = False
    first_result_released = False
    try:
        second_started = second_task_started.wait(timeout=5)
        if second_started:
            gc.collect()
            first_result_released = len(references) == 1 and references[0]() is None
    finally:
        release_second_task.set()
        pipeline_thread.join(timeout=5)

    assert second_started
    assert not pipeline_thread.is_alive()
    assert errors == []
    assert first_result_released


def test_retain_results_does_not_disable_accumulator(dask_client):
    result = sliding_window_pipeline(
        [1, 2, 3],
        simple_no_args,
        dask_client,
        sliding_window_size=1,
        completion_function=simple_completion,
        accumulator=simple_accumulator,
        retain_results=False,
    )

    assert result == 12


def test_empty_input_in_all_return_modes(dask_client):
    assert (
        sliding_window_pipeline([], simple_no_args, dask_client, sliding_window_size=1)
        == []
    )
    assert (
        sliding_window_pipeline(
            [],
            simple_no_args,
            dask_client,
            sliding_window_size=1,
            completion_function=simple_completion,
        )
        == []
    )
    assert (
        sliding_window_pipeline(
            [],
            simple_no_args,
            dask_client,
            sliding_window_size=1,
            completion_function=simple_completion,
            accumulator=simple_accumulator,
        )
        is None
    )
    assert (
        sliding_window_pipeline(
            [],
            simple_no_args,
            dask_client,
            sliding_window_size=1,
            retain_results=False,
        )
        is None
    )
    assert (
        sliding_window_pipeline(
            [],
            simple_no_args,
            dask_client,
            sliding_window_size=1,
            completion_function=simple_completion,
            retain_results=False,
        )
        is None
    )


def test_one_shot_iterable_is_materialized_once(dask_client):
    source = OneShotIterable([1, 1, 2, 3])
    result = sliding_window_pipeline(
        source, simple_no_args, dask_client, sliding_window_size=4
    )

    assert source.iterations == 1
    assert Counter(result) == Counter([2, 2, 3, 4])


def test_accumulator_without_completion_is_rejected_before_submit(
    dask_client, monkeypatch
):
    source = OneShotIterable([1])
    submit_calls = 0
    original_submit = dask_client.submit

    def record_submit(*args, **kwargs):
        nonlocal submit_calls
        submit_calls += 1
        return original_submit(*args, **kwargs)

    monkeypatch.setattr(dask_client, "submit", record_submit)

    with pytest.raises(ValueError, match="requires a completion_function"):
        sliding_window_pipeline(
            source,
            simple_no_args,
            dask_client,
            sliding_window_size=1,
            accumulator=simple_accumulator,
        )

    assert source.iterations == 1
    assert submit_calls == 0


@pytest.mark.parametrize(
    "window_size",
    [True, False, 0, -1, 1.0, "AUTO", "1", None, float("nan"), float("inf")],
)
def test_invalid_sliding_window_size(window_size, dask_client):
    with pytest.raises(ValueError, match="sliding_window_size"):
        sliding_window_pipeline(
            [], simple_no_args, dask_client, sliding_window_size=window_size
        )


@pytest.mark.parametrize(
    "tasks_per_worker",
    [True, False, 0, -1, float("nan"), float("inf"), float("-inf"), "2"],
)
def test_invalid_tasks_per_worker(tasks_per_worker, dask_client):
    with pytest.raises(ValueError, match="task_per_worker"):
        sliding_window_pipeline(
            [],
            simple_no_args,
            dask_client,
            sliding_window_size=1,
            task_per_worker=tasks_per_worker,
        )


def test_invalid_automatic_window_size(dask_client, monkeypatch):
    monkeypatch.setattr(dask_client, "nthreads", lambda: {})
    with pytest.raises(ValueError, match="zero workers"):
        sliding_window_pipeline([], simple_no_args, dask_client)

    monkeypatch.setattr(dask_client, "nthreads", lambda: {"worker": 1})
    with pytest.raises(ValueError, match="rounded below one"):
        sliding_window_pipeline([], simple_no_args, dask_client, task_per_worker=0.25)


def _assert_all_cancelled(futures):
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and any(
        future.status != "cancelled" for future in futures
    ):
        time.sleep(0.01)
    assert futures
    assert all(future.status == "cancelled" for future in futures)


@pytest.mark.parametrize(
    ("failure_stage", "expected_message"),
    [
        ("submit", "submit failed"),
        ("processing", "processing failed"),
        ("completion", "completion failed"),
        ("worker_completion", "completion failed"),
        ("accumulator", "accumulator failed"),
    ],
)
def test_failures_cancel_all_outstanding_futures(
    failure_stage, expected_message, dask_client, monkeypatch
):
    submitted = []
    submit_calls = 0
    original_submit = dask_client.submit

    def record_or_fail_submit(*args, **kwargs):
        nonlocal submit_calls
        submit_calls += 1
        if failure_stage == "submit" and submit_calls == 2:
            raise RuntimeError("submit failed")
        future = original_submit(*args, **kwargs)
        submitted.append(future)
        return future

    monkeypatch.setattr(dask_client, "submit", record_or_fail_submit)

    kwargs = {}
    if failure_stage == "submit":
        items = [(False, 0.3)] * 3
        processing_function = fail_or_delay
    elif failure_stage == "processing":
        items = [(True, 0), (False, 0.3), (False, 0.3)]
        processing_function = fail_or_delay
    else:
        items = [("fail", 0), ("slow-1", 0.3), ("slow-2", 0.3)]
        processing_function = delayed_value
        kwargs["completion_function"] = (
            fail_completion
            if failure_stage in ("completion", "worker_completion")
            else lambda value: value
        )
        if failure_stage == "worker_completion":
            kwargs["completion_on_worker"] = True
        if failure_stage == "accumulator":
            kwargs["accumulator"] = fail_accumulator

    with pytest.raises(RuntimeError, match=expected_message):
        sliding_window_pipeline(
            items,
            processing_function,
            dask_client,
            sliding_window_size=3,
            **kwargs,
        )

    _assert_all_cancelled(submitted)
    # Cancellation cannot interrupt a running Python function.  Let those
    # short tasks leave the shared worker threads before the next case starts.
    time.sleep(0.35)


def test_swp_error_handlers(dask_client):
    """
    pytest function to exercise all error handlers that can be raised by
    the function `sliding_window_pipeline`.  Uses the set of function
    names defined earlier in this file.
    """
    # test arg0 handling
    with pytest.raises(ValueError, match="Illegal value for arg0"):
        result = sliding_window_pipeline(42, simple_no_args, dask_client)
    with pytest.raises(ValueError, match="progress_report_interval"):
        sliding_window_pipeline(
            [], simple_no_args, dask_client, progress_report_interval=0
        )
    with pytest.raises(ValueError, match="retain_results"):
        sliding_window_pipeline([], simple_no_args, dask_client, retain_results="false")
    with pytest.raises(ValueError, match="completion_on_worker must be a boolean"):
        sliding_window_pipeline(
            [], simple_no_args, dask_client, completion_on_worker="true"
        )
    with pytest.raises(ValueError, match="requires a completion_function"):
        sliding_window_pipeline(
            [], simple_no_args, dask_client, completion_on_worker=True
        )
    # test arg1 handling
    with pytest.raises(
        ValueError,
        match="Illegal value for arg1 - must be the name of a processing function",
    ):
        result = sliding_window_pipeline([], "foobar", dask_client)
    # test arg2 handling
    with pytest.raises(ValueError, match="Illegal value for arg2"):
        result = sliding_window_pipeline([], simple_no_args, "foobar")
    # test handling of pfunc_args and pfunc_kwargs
    with pytest.raises(ValueError, match="Illegal input for pfunc_args."):
        result = sliding_window_pipeline([], simple_full, dask_client, pfunc_args=42)
    with pytest.raises(ValueError, match="Illegal input for pfunc_kwargs."):
        result = sliding_window_pipeline(
            [], simple_no_args, dask_client, pfunc_kwargs=42
        )
    # similar tests for completion function and args
    with pytest.raises(
        ValueError, match="Illegal value for completion_function argument"
    ):
        result = sliding_window_pipeline(
            [], simple_no_args, dask_client, completion_function="foobar"
        )
    with pytest.raises(ValueError, match="Illegal input for cfunc_args."):
        result = sliding_window_pipeline(
            [],
            simple_no_args,
            dask_client,
            completion_function=simple_completion,
            cfunc_args=42,
        )
    with pytest.raises(ValueError, match="Illegal input for cfunc_kwargs."):
        result = sliding_window_pipeline(
            [],
            simple_no_args,
            dask_client,
            completion_function=simple_completion,
            cfunc_kwargs=42,
        )
    # similar tests for accumulation function and args
    with pytest.raises(ValueError, match="Illegal value for accumulator argument"):
        result = sliding_window_pipeline(
            [],
            simple_no_args,
            dask_client,
            completion_function=simple_completion,
            accumulator="foobar",
        )
    with pytest.raises(ValueError, match="Illegal input for a_args."):
        result = sliding_window_pipeline(
            [],
            simple_no_args,
            dask_client,
            completion_function=simple_completion,
            accumulator=simple_accumulator,
            a_args=42,
        )
    with pytest.raises(ValueError, match="Illegal input for a_kwargs."):
        result = sliding_window_pipeline(
            [],
            simple_no_args,
            dask_client,
            completion_function=simple_completion,
            accumulator=simple_accumulator,
            a_kwargs=42,
        )
