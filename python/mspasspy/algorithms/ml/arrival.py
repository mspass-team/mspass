import numpy as np
import seisbench.models as sbm

from seisbench.models.base import WaveformModel
from obspy.taup import TauPyModel
from obspy.geodetics import gps2dist_azimuth, kilometers2degrees
from mspasspy.ccore.seismic import TimeSeries, TimeSeriesEnsemble
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.algorithms.window import WindowData


def annotate_arrival_time(
    timeseries: TimeSeries,
    threshold=0.2,
    time_window: TimeWindow = None,
    model: WaveformModel = None,
    model_args: dict = None,
):
    """
    Predict the arrival time of the P wave using the provided seisbench WaveformModel.
    The arrival time will saved as metadata in the input TimeSeries object and can be accessed using \
        the key 'p_wave_picks'. Time is stored in the relative format using the start time of the TimeSeries.

    :param timeseries: The time series data to predict the arrival time.
    :param threshold: The probability threshold (0-1) to filter p-wave picks. Default value is 0.2.
    :param time_window: The relative time window to filter the predicted arrival time. If not provided, the whole time series will be used.
    :param model: The model used to predict the arrival time.
    :param model_args: arguments to initialize a new model if not provided
    :type timeseries: mspasspy.ccore.seismic.TimeSeries
    :type threshold: float
    :type time_window: mspasspy.ccore.algorithms.basic.TimeWindow
    :type model: seisbench.models.base.WaveformModel
    :type model_args: dict
    """

    # Check the input arguments
    if not 0 <= threshold <= 1:
        raise ValueError("Threshold must be between 0 and 1")

    # load pretrained model based on the args if not provided
    if model == None:
        # 'stead' model was trained on STEAD for 100 epochs with a learning rate of 0.01.
        # use sbm.PhaseNet.list_pretrained(details=True) to list out other supported models
        # when using this model, please reference the SeisBench publications listed at https://github.com/seisbench/seisbench
        pretrained_model = "stead" if (model_args == None or "name" not in model_args) else model_args["name"]
        model = sbm.PhaseNet.from_pretrained(pretrained_model)

    # apply the window if provided and convert time series to stream
    windowed_timeseries = (
        WindowData(timeseries, time_window.start, time_window.end)
        if time_window
        else timeseries
    )
    ts_ensemble = TimeSeriesEnsemble()
    ts_ensemble.member.append(windowed_timeseries)
    stream = ts_ensemble.toStream()

    # prediction result is the probability for picks over time
    pred_st = model.annotate(stream)

    # Step 1: Access the probability data
    for tr in pred_st:
        if tr.stats.channel == "PhaseNet_P":
            trace = tr
            break
    data = trace.data

    # Step 2: Find all the index with probability value greater than the threshold
    indices = np.where(data > threshold)[0]

    # Step 3: Calculate the corresponding time
    # Trace start time
    start_time = trace.stats.starttime # obspy.UTCDateTime
    # Sampling rate (samples per second)
    sampling_rate = trace.stats.sampling_rate
    # Calculate time offset and arrival time of picks in seconds
    time_offsets = indices / sampling_rate
    breakpoint()
    p_wave_picks = start_time.timestamp + time_offsets

    # Step 4: Save the arrival time in the relative format
    # Adjust the arrival time if a time window is provided
    if time_window:
        p_wave_picks += time_window.start
    timeseries["p_wave_picks"] = p_wave_picks
