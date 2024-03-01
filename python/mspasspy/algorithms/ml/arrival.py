import numpy as np
import seisbench.models as sbm

from obspy.taup import TauPyModel
from obspy.geodetics import gps2dist_azimuth, kilometers2degrees
from mspasspy.ccore.seismic import TimeSeries, TimeSeriesEnsemble
from seisbench.models.base import WaveformModel


def annotate_arrival_time(
    timeseries: TimeSeries, model: WaveformModel = None, model_args: dict = None
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

    # Step 2: Find the index of the maximum probability value
    max_index = np.argmax(data)

    # Step 3: Calculate the corresponding time
    # Trace start time
    start_time = trace.stats.starttime
    # Sampling rate (samples per second)
    sampling_rate = trace.stats.sampling_rate
    # Calculate time offset in seconds
    time_offset = max_index / sampling_rate
    # Calculate the absolute time of the highest probability
    arrival_time = start_time + time_offset

    # Convert to a more convenient format if necessary, e.g., UTCDateTime to string
    arrival_time_str = arrival_time.strftime("%Y-%m-%d %H:%M:%S.%f")

    timeseries["p_time"] = arrival_time_str


# TODO fix me
def predict_P_wave_arrival_time_obspy(d: TimeSeries):
    model = TauPyModel(model="iasp91")
    srclat = d["source_lat"]
    srclon = d["source_lon"]
    srcdep = d["source_depth"]
    srctime = d["source_time"]
    stalat = d["channel_lat"]
    stalon = d["channel_lon"]
    staelev = d["channel_elev"]
    georesult = gps2dist_azimuth(srclat, srclon, stalat, stalon)
    degdist = kilometers2degrees(georesult[0] / 1000.0)

    arr = model.get_travel_times(
        distance_in_degree=degdist, source_depth_in_km=srcdep, phase_list=["P"]
    )
    atime = srctime + arr[0].time
    d["Ptime"] = atime
