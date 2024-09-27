import logging
import numpy as np
import seisbench.models as sbm

from seisbench.models.base import WaveformModel
from mspasspy.ccore.seismic import TimeSeries, TimeSeriesEnsemble
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.algorithms.window import WindowData
from obspy import UTCDateTime


def annotate_arrival_time(
    timeseries: TimeSeries,
    threshold=0.2,
    time_window: TimeWindow = None,
    model: WaveformModel = None,
    model_args: dict = None,
):
    """
    Predict the arrival time of the P wave using the provided seisbench WaveformModel.
    The arrival time will be saved as a dictionary in the input TimeSeries object and can be accessed using \
        the key 'p_wave_picks'. In the dictionay, the key is the arrival time in the utc timestamp format, \ 
        and the value is the probability of the pick.

    :param timeseries: The time series data to predict the arrival time.
    :param threshold: The probability threshold (0-1) to filter p-wave picks. \ 
        Any picks with probability less than the threshold will be removed. Default value is 0.2. 
    :param time_window: The relative time window to filter the predicted arrival time. \ 
        If not provided, the whole time series will be used.
    :param model: The model used to predict the arrival time.
    :param model_args: arguments to initialize a new model if not provided
    :type timeseries: mspasspy.ccore.seismic.TimeSeries
    :type threshold: float
    :type time_window: mspasspy.ccore.algorithms.basic.TimeWindow defined as absolute time in UTC
    :type model: seisbench.models.base.WaveformModel
    :type model_args: dict
    """

    default_threshold = 0.2

    # Check the input arguments
    if not 0 <= threshold <= 1:
        logging.warning('Threshold should be in the range of [0, 1]. Using default threshold {}}'.format(default_threshold))
        threshold = default_threshold

    # convert timeseries to absolute time
    timeseries.rtoa()

    # load pretrained model based on the args if not provided
    if model == None:
        # 'stead' model was trained on STEAD for 100 epochs with a learning rate of 0.01.
        # use sbm.PhaseNet.list_pretrained(details=True) to list out other supported models
        # when using this model, please reference the SeisBench publications listed at https://github.com/seisbench/seisbench
        pretrained_model = "stead" if (model_args == None or "name" not in model_args) else model_args["name"]
        model = sbm.PhaseNet.from_pretrained(pretrained_model)

    ts_ensemble = TimeSeriesEnsemble()
    ts_ensemble.member.append(timeseries)
    stream = ts_ensemble.toStream()

    # apply the window if provided and convert time series to stream
    start_time_utc = stream[0].stats.starttime.timestamp # UTC timestamp
    end_time_utc = stream[0].stats.endtime.timestamp # UTC timestamp

    # adjust the time window if it is out of the time range of the time series
    if time_window:
        if time_window.end < start_time_utc or time_window.start > end_time_utc:
            time_window.start = start_time_utc
            time_window.end = end_time_utc
            logging.warning('Time window is out of the time range of the time series. Adjusting the time window to the time range of the time series.')
        if time_window.end > end_time_utc:
            time_window.end = end_time_utc
        if time_window.start < start_time_utc:
            time_window.start = start_time_utc
    
    windowed_stream = (
        stream.trim(UTCDateTime(time_window.start), UTCDateTime(time_window.end)) \
        if time_window else stream
    )

    # prediction result is the probability for picks over time
    pred_st = model.annotate(windowed_stream)

    # Step 1: Access the probability data
    for tr in pred_st:
        if tr.stats.channel == "PhaseNet_P":
            trace = tr
            break
    data = trace.data

    # Step 2: Find all the index with probability value greater than the threshold
    indices = np.where(data >= threshold)[0]

    # Step 3: Calculate the corresponding time in utc timestamp
    timestamps = trace.times("timestamp")[indices]

    # Step 4: Create a dictionary with timestamps as keys and probability values as values
    p_wave_picks = {ts: data[i] for ts, i in zip(timestamps, indices)}

    # Step 5: Save the arrival time dictionary in absolute time
    timeseries["p_wave_picks"] = p_wave_picks
