import os
import shutil
import obspy

'''
def lambda_func(input_path, event):
    # after processing, return output_path
    # arg1 = event['arg1']
    # arg2 = event['arg2']
    # output_path = preprocess(intput_path)
    # return output_path
    pass
'''

def lambda_func(input_path, event):
    duration = -1
    t0shift = 0
    if 'duration' in event:
        duration = event['duration']
    if 't0shift' in event:
        t0shift = event['t0shift']
        
    basename = os.path.basename(input_path)
    (filename, ext) = os.path.splitext(basename)
    filename = filename + "_{}_{}".format(duration, t0shift) + ext
    outfile = '/tmp/' + filename

    if(duration == -1): #   Do nothing, just copy the file
        print('Not window, because not given params')
        shutil.copyfile(input_path, outfile)
        return outfile
    
    print('Start handleing ', input_path)
    st = obspy.read(input_path)
    start_time = st[0].stats['starttime'] + t0shift
    end_time = start_time + duration
    st.trim(start_time, end_time)

    print('Window done, writing', outfile)
    st.write(outfile, 'MSEED')
    return outfile