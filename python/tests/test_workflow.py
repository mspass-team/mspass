import pytest
import dask.distributed as ddist
import numpy as np
from mspasspy.workflow import sliding_window_pipeline

# run functions used for tests
def simple_no_args(a):
    #ddist.print("Running a=",a)
    b=a+1
    return b

def simple_warg(a,b):
    #ddist.print(f"Running with {a=} and {b=}")
    x=a+b
    return x

def simple_wkwarg(a,b=2):
    #ddist.print(f"Running with {a=} and {b=}")
    x=a+b
    return x

def simple_full(a,b,c,d=5):
    #ddist.print(f"Running with {a=}, {b=}, {c=}")
    #ddist.print("Running with kwarg value {d=}")
    x=a+b+c+d
    return x

def simple_completion(a):
    return a+1
def completion_full(a,b,c=10):
    ddist.print(a,b,c)
    return a+b+c

def simple_accumulator(old,x):
    print(f"{old=} {x=}")
    if old is None:
        return x
    else:
        return old+x
def accumulator_full(old,x,a,b=30):
    print(f"{old=} {x=} {a=} {b=}")
    if old is None:
        return x + a + b
    else:
        print("accmulator_full return=",old+x+a+b)
        return old + x + a + b

cluster = ddist.LocalCluster(processes=False)  # processes=False is necessary to run this locally with pytest
dask_client = cluster.get_client()

def test_sliding_window_pipeline():
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
    listsize=10
    # use random integers so tests are unlikely to randomly work or fail
    # necessary as the processing functions are all trivial integer arithmetic operations
    dlist=np.random.randint(1,5000,size=listsize)
    result = sliding_window_pipeline(dlist,simple_no_args,dask_client,sliding_window_size=4)
    #print(result)
    expected_out=dlist + 1   # numpy vector overload makes this a simple way to create this
    
    for x in result:
        assert x in expected_out
    # repeat with verbose on and sliding_window_size set auto
    result = sliding_window_pipeline(dlist,simple_no_args,dask_client,sliding_window_size="auto",verbose=True)
    for x in result:
        assert x in expected_out
        
    # run function with an arg to pfunc_args
    result = sliding_window_pipeline(dlist, simple_warg, dask_client, sliding_window_size=4, pfunc_args=[2])
    expected_out = dlist + 2
    for x in result:
        assert x in expected_out
        
    # same with pfun_kwarg
    kwa = {"b" : 3}
    result = sliding_window_pipeline(dlist, simple_wkwarg, dask_client, sliding_window_size=4,pfunc_kwargs=kwa)
    expected_out = dlist + 3
    for x in result:
        assert x in expected_out
        
    # similar but let kwargs default
    result = sliding_window_pipeline(dlist, simple_wkwarg, dask_client, sliding_window_size=4)
    expected_out = dlist + 2  # note 2 must match default of function
    for x in result:
        assert x in expected_out
        
    # run function with multiple args
    kwa = {"d" : 4}
    result = sliding_window_pipeline(dlist, simple_full, dask_client, sliding_window_size=4, pfunc_args=[1,2],pfunc_kwargs=kwa)
    expected_out = dlist + 7 # sum of all 3 args passed
    for x in result:
        assert x in expected_out
        
    # run same with a completion that just adds 1 with no args
    result = sliding_window_pipeline(dlist, simple_full, dask_client, sliding_window_size=4, 
                                     pfunc_args=[1,2],pfunc_kwargs=kwa,
                                     completion_function=simple_completion)
    expected_out = dlist + 7 + 1 # sum of all 3 args passed + completion add 1
    for x in result:
        assert x in expected_out
        
    # now run with arg and default kwarg
    result = sliding_window_pipeline(dlist, simple_full, dask_client, sliding_window_size=4, 
                                     pfunc_args=[1,2],pfunc_kwargs=kwa,
                                     completion_function=completion_full,
                                     cfunc_args=[20])
    addamount = 7 + 20 + 10
    expected_out = dlist + addamount
    for x in result:
        assert x in expected_out
        
    # now add cfunc kwarg
    result = sliding_window_pipeline(dlist, simple_full, dask_client, sliding_window_size=4, 
                                     pfunc_args=[1,2],pfunc_kwargs=kwa,
                                     completion_function=completion_full,
                                     cfunc_args=[20],cfunc_kwargs={"c" : 30})
    addamount = 7 + 20 + 30 
    expected_out = dlist + addamount
    for x in result:
        assert x in expected_out
    
    # finally test accmulator feature
    # change the input in this case as all we care about this the summed output 
    for i in range(len(dlist)):
        dlist[i] = i
    print("Testing simple_accumulator")
    result = sliding_window_pipeline(dlist, simple_full, dask_client, sliding_window_size=4, 
                                     pfunc_args=[1,2],pfunc_kwargs={"d" : 10},
                                     completion_function=simple_completion,
                                     accumulator=simple_accumulator)
    expected_result = 0
    for i in range(len(dlist)):
        expected_result = expected_result + dlist[i] + 1 + 2 + 10 + 1 
    assert result==expected_result
    
    # add a_args
    print("testing accumulator_full with arg set and default kwarg")
    result = sliding_window_pipeline(dlist, simple_full, dask_client, sliding_window_size=4, 
                                     pfunc_args=[1,2],pfunc_kwargs={"d" : 10},
                                     completion_function=simple_completion,
                                     accumulator=accumulator_full,
                                     a_args=[5])
    expected_result = 0
    for i in range(len(dlist)):
        expected_result = expected_result + dlist[i] + 1 + 2 + 10 + 1 + 5 + 30
    assert result==expected_result
    
    # add a_kwargs
    result = sliding_window_pipeline(dlist, simple_full, dask_client, sliding_window_size=4, 
                                     pfunc_args=[1,2],pfunc_kwargs={"d" : 10},
                                     completion_function=simple_completion,
                                     accumulator=accumulator_full,
                                     a_args=[5],
                                     a_kwargs={"b" : 42})
    expected_result = 0
    for i in range(len(dlist)):
        expected_result = expected_result + dlist[i] + 1 + 2 + 10 + 1 + 5 + 42
    assert result==expected_result
def test_swp_error_handlers():
    """
    pytest function to exercise all error handlers that can be raised by 
    the function `sliding_window_pipeline`.  Uses the set of function 
    names defined earlier in this file.
    """
    # test arg0 handling
    with pytest.raises(ValueError,match="Illegal value for arg0"):
        result = sliding_window_pipeline(42, simple_no_args, dask_client)
    # test arg1 handling
    with pytest.raises(ValueError,match="Illegal value for arg1 - must be the name of a processing function"):
        result = sliding_window_pipeline([], "foobar", dask_client)
    # test arg2 handling
    with pytest.raises(ValueError,match="Illegal value for arg2"):
        result = sliding_window_pipeline([], simple_no_args, "foobar")
    # test handling of pfunc_args and pfunc_kwargs
    with pytest.raises(ValueError,match="Illegal input for pfunc_args."):
        result = sliding_window_pipeline([], simple_full, dask_client, pfunc_args=42)
    with pytest.raises(ValueError,match="Illegal input for pfunc_kwargs."):
        result = sliding_window_pipeline([], simple_no_args, dask_client, pfunc_kwargs=42)
    # similar tests for completion function and args
    with pytest.raises(ValueError,match="Illegal value for completion_function argument"):
        result = sliding_window_pipeline([], simple_no_args, dask_client,completion_function="foobar")
    with pytest.raises(ValueError,match="Illegal input for cfunc_args."):
        result = sliding_window_pipeline([], simple_no_args, dask_client, completion_function=simple_completion,cfunc_args=42)
    with pytest.raises(ValueError,match="Illegal input for cfunc_kwargs."):
        result = sliding_window_pipeline([], simple_no_args, dask_client, completion_function=simple_completion,cfunc_kwargs=42)   
    # similar tests for accumulation function and args
    with pytest.raises(ValueError,match="Illegal value for accumulator argument"):
        result = sliding_window_pipeline([], simple_no_args, dask_client,completion_function=simple_completion,accumulator="foobar")
    with pytest.raises(ValueError,match="Illegal input for a_args."):
        result = sliding_window_pipeline([], simple_no_args, dask_client, completion_function=simple_completion,accumulator=simple_accumulator,a_args=42)
    with pytest.raises(ValueError,match="Illegal input for a_kwargs."):
        result = sliding_window_pipeline([], simple_no_args, dask_client, completion_function=simple_completion,accumulator=simple_accumulator,a_kwargs=42)