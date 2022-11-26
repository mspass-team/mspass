from mspasspy.ccore.algorithms.deconvolution import MTPowerSpectrumEngine
from mspasspy.ccore.seismic import TimeSeries,TimeReferenceType
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.algorithms.snr import (snr,
                                     FD_snr_estimator,
                                     arrival_snr,
                                     arrival_snr_QC)
import numpy as np
from scipy import signal
from bson import json_util
import pickle

def build_signal(T,dt=0.01,shift_factor=0.75,npoles=3,corners=[0.1,5.0],nscale=0.001):
    ts=TimeSeries()
    N=int(T/dt)
    ts.set_npts(N)
    ts.dt=dt
    ts.t0=0.0
    ts.set_live()
    # Create a spike 75% of the way through the window and set that 
    # point as t0.  then we filter 
    ispike=int(N*shift_factor)
    t0_shift = ispike*dt
    ts.data[ispike]=1.0
    ts.t0 = -t0_shift
    sos=signal.butter(npoles,corners,btype='bandpass',output='sos',fs=1.0/dt)
    y=signal.sosfilt(sos,ts.data)
    #rescale so peak has amplitude of 1
    dmax=np.max(y)
    y/=dmax
    for i in range(ts.npts):
        ts.data[i] = y[i] + nscale*np.random.normal()
    return ts
def verify_snr_outputs_match(so1,so2):
    """
    Runs through a set of comptuted snr metric keys comparing 
    values in so1 and so2.  This function is used to compare 
    several calls to variations of SD_snr_estimator with different parameters
    that should all yield the same answer. 
    """
    keylist=[
        "low_f_band_edge",
        "high_f_band_edge",
        "low_f_band_edge_snr",
        "high_f_band_edge_snr",
        "spectrum_frequency_range",
        "bandwidth_fraction",
        "bandwidth",
        "mean_snr",
        "maximum_snr",
        "median_snr",
        "minimum_snr",
        "q3_4_snr",
        "q1_4_snr",
        "snr_envelope_Linf_over_L1",
        "snr_L2",
        "snr_Linf",
        "snr_perc",
        "snr_MAD"
        ]
    for k in keylist:
        assert np.isclose(so1[k],so2[k])
        
    
def test_snr():
    ts = build_signal(300)
    t=[]
    for i in range(ts.npts):
        tt = ts.time(i)
        t.append(tt)
    nwin=TimeWindow(-200.0,-50.0)
    # appropriate for perc and peak
    swin=TimeWindow(-10.0,50.0)
    # used for rms and mad for this test
    swin2=TimeWindow(-0.5,10.0)
    
    snrrms = snr(ts,nwin,swin2,noise_metric="rms",signal_metric="rms")
    print("snr_rms=",snrrms)
    snrpeak_rms = snr(ts,nwin,swin,noise_metric="rms",signal_metric="peak")
    print("snr_peak-l2=",snrpeak_rms)
    tscpy=TimeSeries(ts)
    snrmad_rms = snr(tscpy,nwin,swin2,noise_metric="rms",signal_metric="mad")
    print("snr_mad-L2=",snrmad_rms)
    snrperc_rms = snr(ts,nwin,swin,noise_metric="rms",signal_metric="perc",perc=0.95)
    print("snr_perc95-L2=",snrperc_rms)
    assert snrrms>80 and snrrms<95
    assert snrpeak_rms>980 and snrpeak_rms<1020
    assert snrmad_rms>5 and snrmad_rms<7
    assert snrperc_rms>10 and snrperc_rms<12
    print("Output of FD_snr_estimator with default parameters")
    fd_snr_output = FD_snr_estimator(ts,noise_window=nwin,signal_window=swin,
                        high_frequency_search_start=30.0,fix_high_edge=False)
    print(json_util.dumps(fd_snr_output[0],indent=2))
    elog = fd_snr_output[1]
    assert elog.size()==0
    tval = fd_snr_output[0]["low_f_band_edge"]
    assert tval>0.02 and tval<0.04
    tval = fd_snr_output[0]["high_f_band_edge"]
    assert tval>12 and tval<18
    tval = fd_snr_output[0]["low_f_band_edge_snr"]
    assert tval>2 and tval<30
    tval = fd_snr_output[0]["high_f_band_edge_snr"]
    assert tval>2 and tval<30
    # Note this is not 50 because the signal window npts is an odd number
    # In that sitaution ffts have last frequecy Nyqust - df/2
    tval = fd_snr_output[0]["spectrum_frequency_range"]
    assert np.isclose(tval,49.991668053)
    tval = fd_snr_output[0]["bandwidth_fraction"]
    assert tval>0.25 and tval<0.45
    tval = fd_snr_output[0]["bandwidth"]
    assert tval>50 and tval<60
    
    print("Repeat computing optional metrics and fixed high band edge")
    fd_snr_output = FD_snr_estimator(ts,noise_window=nwin,signal_window=swin,
                            optional_metrics=['snr_stats','filtered_envelope','filtered_L2','filtered_Linf','filtered_MAD','filtered_perc'])
    print(json_util.dumps(fd_snr_output[0],indent=2))
    elog = fd_snr_output[1]
    assert elog.size()==0
    tval = fd_snr_output[0]["low_f_band_edge"]
    assert tval>0.02 and tval<0.04
    tval = fd_snr_output[0]["high_f_band_edge"]
    assert np.isclose(tval,2.0)
    tval = fd_snr_output[0]["low_f_band_edge_snr"]
    assert tval>2 and tval<30
    tval = fd_snr_output[0]["high_f_band_edge_snr"]
    assert tval>2 and tval<100
    # Note this is not 50 because the signal window npts is an odd number
    # In that sitaution ffts have last frequecy Nyqust - df/2
    tval = fd_snr_output[0]["spectrum_frequency_range"]
    assert np.isclose(tval,49.991668053)
    tval = fd_snr_output[0]["bandwidth_fraction"]
    assert tval>0.02 and tval<0.05
    tval = fd_snr_output[0]["bandwidth"]
    assert tval>30 and tval<40
    # optional metric validation
    tval = fd_snr_output[0]["mean_snr"]
    assert tval>55 and tval<75
    tval = fd_snr_output[0]["maximum_snr"]
    assert tval>90 and tval<150
    tval = fd_snr_output[0]["median_snr"]
    assert tval>55 and tval<75
    tval = fd_snr_output[0]["minimum_snr"]
    assert tval>2 and tval<30
    tval = fd_snr_output[0]["q3_4_snr"]
    assert tval>60 and tval<100
    tval = fd_snr_output[0]["q1_4_snr"]
    assert tval>40 and tval<70
    tval = fd_snr_output[0]["stats_are_valid"]
    assert tval
    
    master=fd_snr_output[0]
    
    print("Repeat testing save_spectrum option")
    # This one is for interactive testing - do no include in pytest
    fd_snr_output = FD_snr_estimator(ts,noise_window=nwin,signal_window=swin,save_spectra=True)
    o=fd_snr_output[0]
    pd=o["signal_spectrum"]
    sigspec=pickle.loads(pd)
    pd=o["noise_spectrum"]
    nspec=pickle.loads(pd)
    # We just validate these are intact.   If this method succeeds assume 
    # that worked
    assert sigspec.nf()==3000
    assert nspec.nf()==7500
    
    print("Testing arrival_snr function with autoshift")
    # Now test arrival_snr.  That function is mainly a front end to 
    # FD_snr_estimator to handle time shifting for an arrival window.  
    # Test is then just equality with the previous output
    # We just change t0 but don't mess with time reference as it isn't 
    # required here.  Beware that could change down the road and 
    # break this test as that is an implementation detail
    ts2=TimeSeries(ts)
    ts2.t0 = 100000.0 + ts.t0
    ts2['Ptime']=100000.0
    ts2.tref=TimeReferenceType.UTC
    # Test auto shift of window when data are utc
    asnr_out=arrival_snr(ts2,noise_window=nwin,signal_window=swin)
    print(json_util.dumps(asnr_out["Parrival"],indent=2))
    verify_snr_outputs_match(master,asnr_out["Parrival"])
    
    print("Testing same with window shift applied before calling")
    nwin2=nwin.shift(100000.0)
    swin2=swin.shift(100000.0)
    asnr_out2=arrival_snr(ts2,noise_window=nwin2,signal_window=swin2)
    print(json_util.dumps(asnr_out2["Parrival"],indent=2))
    verify_snr_outputs_match(asnr_out["Parrival"],asnr_out2["Parrival"])
    
    print("Testing arrival_snr_QC variant")
    asnr_out3 = arrival_snr_QC(ts2,noise_window=nwin,signal_window=swin,use_measured_arrival_time=True)
    print(json_util.dumps(asnr_out3["Parrival"],indent=2))
    verify_snr_outputs_match(asnr_out["Parrival"],asnr_out3["Parrival"])
