from mspasspy.util.decorators import mspass_func_wrapper

@mspass_func_wrapper
def ator(data, tshift, *args,
        object_history=False, alg_name=None, alg_id=None, dryrun=False,
        inplace_return=True, function_return_key=None, **kwargs):
    data.ator(tshift)

@mspass_func_wrapper
def rtoa(data, *args,
        object_history=False, alg_name=None, alg_id=None, dryrun=False,
        inplace_return=True, function_return_key=None, **kwargs):
    data.rtoa()

@mspass_func_wrapper
def rotate(data, rotate_param, *args,
        object_history=False, alg_name=None, alg_id=None, dryrun=False,
        inplace_return=True, function_return_key=None, **kwargs):
    data.rotate(rotate_param)

@mspass_func_wrapper
def rotate_to_standard(data, *args,
        object_history=False, alg_name=None, alg_id=None, dryrun=False,
        inplace_return=True, function_return_key=None, **kwargs):
    data.rotate_to_standard()

@mspass_func_wrapper
def free_surface_transformation(data, uvec, a0, b0, *args,
        object_history=False, alg_name=None, alg_id=None, dryrun=False,
        inplace_return=True, function_return_key=None, **kwargs):
    data.free_surface_transformation(uvec, a0, b0)

@mspass_func_wrapper
def transform(data, matrix, *args,
        object_history=False, alg_name=None, alg_id=None, dryrun=False,
        inplace_return=True, function_return_key=None, **kwargs):
    data.transform(matrix)