import numpy as np
import seisbench.models as sbm

from obspy.taup import TauPyModel
from obspy.geodetics import gps2dist_azimuth, kilometers2degrees
from mspasspy.ccore.seismic import TimeSeries, TimeSeriesEnsemble
from seisbench.models.base import WaveformModel


def annotate_arrival_time(
    timeseries: TimeSeries,
    threshold=0.2,
    window_start_offset_sec=0,
    window_length_sec=None,
    model: WaveformModel = None,
    model_args: dict = None,
):
    """
    Predict the arrival time of the seismic phase using the provided seisbench WaveformModel.
    The arrival time will added to the input TimeSeries object and can be accessed using the key 'p_time'.

    :param timeseries: The time series data to predict the arrival time.
    :param model: The model used to predict the arrival time.
    :param model_args: arguments to initialize a new model if not provided
    :type timeseries: mspasspy.ccore.seismic.TimeSeries
    :type model: seisbench.models.base.WaveformModel
    :type model_args: dict
    """
    # load pretrained model based on the args if not provided
    if model == None:
        # 'stead' model was trained on STEAD for 100 epochs with a learning rate of 0.01.
        # use sbm.PhaseNet.list_pretrained(details=True) to list out other supported models
        # when using this model, please reference the SeisBench publications listed at https://github.com/seisbench/seisbench
        pretrained_model = "stead" if model_args["name"] == None else model_args["name"]
        model = sbm.PhaseNet.from_pretrained(pretrained_model)

    # convert time series to stream
    ts_ensemble = TimeSeriesEnsemble()
    ts_ensemble.member.append(timeseries)
    stream = ts_ensemble.toStream()

    # prediction result is the probability for picks over time
    pred_st = model.annotate(stream)

    # Step 1: Access the data
    for tr in pred_st:
        if trace.stats.channel == "PhaseNet_P":
            trace = tr
            break
    data = trace.data

    # Step 2: Find all the index with probability value greater than the threshold
    indices = np.where(data > threshold)[0]

    # Step 3: Calculate the corresponding time
    # Trace start time
    start_time = trace.stats.starttime
    window_start = trace.stats.starttime + window_start_offset_sec
    window_end = window_start + window_length_sec if window_length_sec else trace.stats.endtime
    if not window_start < window_end:
        raise ValueError("Invalid time window")

    # Sampling rate (samples per second)
    sampling_rate = trace.stats.sampling_rate
    # Calculate time offset in seconds
    time_offsets = indices / sampling_rate
    # Calculate the absolute time of the highest probability
    arrival_times = start_time + time_offsets

    # Step 4: Filter the indices and arrival times based on the time window
    filtered_indices = []
    filtered_arrival_times = []
    for index, arrival_time in zip(indices, arrival_times):
        if window_start <= arrival_time <= window_end:
            filtered_indices.append(index)
            filtered_arrival_times.append(arrival_time)

    # Step 5: Convert arrival times to a more convenient format if necessary
    filtered_arrival_time_strs = [time.strftime("%Y-%m-%d %H:%M:%S.%f") for time in filtered_arrival_times]

    timeseries["p_arrival_picks_time"] = filtered_arrival_time_strs
