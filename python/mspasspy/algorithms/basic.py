from mspasspy.util.decorators import mspass_func_wrapper

@mspass_func_wrapper
def ator(data, tshift,
        object_history=False, alg_name=None, alg_id=None, dryrun=False,
        inplace_return=True, function_return_key=None):
    data.ator(tshift)

@mspass_func_wrapper
def rtoa(data,
        object_history=False, alg_name=None, alg_id=None, dryrun=False,
        inplace_return=True, function_return_key=None):
    data.rtoa()

@mspass_func_wrapper
def rotate(data, rotate_param,
        object_history=False, alg_name=None, alg_id=None, dryrun=False,
        inplace_return=True, function_return_key=None):
    data.rotate(rotate_param)

@mspass_func_wrapper
def rotate_to_standard(data,
        object_history=False, alg_name=None, alg_id=None, dryrun=False,
        inplace_return=True, function_return_key=None):
    data.rotate_to_standard()

@mspass_func_wrapper
def free_surface_transformation(data, uvec, a0, b0,
        object_history=False, alg_name=None, alg_id=None, dryrun=False,
        inplace_return=True, function_return_key=None):
    data.free_surface_transformation(uvec, a0, b0)

@mspass_func_wrapper
def transform(data, matrix,
        object_history=False, alg_name=None, alg_id=None, dryrun=False,
        inplace_return=True, function_return_key=None):
    data.transform(matrix)