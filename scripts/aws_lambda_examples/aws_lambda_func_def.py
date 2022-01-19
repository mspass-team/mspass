import os
import shutil
import obspy

'''
def lambda_func(input_path, event):
    """
    The template for user-defined lambda function.

    The maximum running time of one execution is 15 mins, so the lambda function can’t be used to do heavy work. 
    The main point of lambda function is to help do some trivial preprocessing on data on aws s3. If some heavy
    calculations are to be done, the better way is to download the data and do it locally.

    :param input_path: The path of the input file.
    :param event: The dictionary passed by "process" function, should be the same dict in request used when calling
        "AwsLambdaClient.call_lambda_function". The user-specified parameters can be extracted as follows:
            arg1 = event['arg1']
            arg2 = event['arg2']
    :return: a dict that contain two elements:
        1) ret_type: two possible value: ‘key’ or ‘value’, 
            ‘key’ means that the output object is saved to some place in s3.
            ‘value’ means that the output object is directly returned through payload
        2) ret_value:
            If ret_type=’key’, ret_value will be the key of the output object in s3.
            If ret_type=’value’, ret_value will be the bytes of the returning object.
    """
'''


def lambda_func(input_path, event):
    # A simple example. This function will do the timewindow on the input file.
    # Users should replace this function with their own function following the template above.
    duration = -1
    t0shift = 0
    if "duration" in event:
        duration = event["duration"]
    if "t0shift" in event:
        t0shift = event["t0shift"]

    basename = os.path.basename(input_path)
    (filename, ext) = os.path.splitext(basename)
    filename = filename + "_{}_{}".format(duration, t0shift) + ext
    outfile = "/tmp/" + filename

    if duration == -1:  #   Do nothing, just copy the file
        print("Not window, because not given params")
        shutil.copyfile(input_path, outfile)
        return outfile

    print("Start handleing ", input_path)
    st = obspy.read(input_path)
    start_time = st[0].stats["starttime"] + t0shift
    end_time = start_time + duration
    st.trim(start_time, end_time)

    print("Window done, writing", outfile)
    st.write(outfile, "MSEED")
    return outfile
