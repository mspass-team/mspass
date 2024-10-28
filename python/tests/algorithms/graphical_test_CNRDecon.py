from mspasspy.ccore.utility import AntelopePf
from mspasspy.ccore.algorithms.deconvolution import CNRDeconEngine
from mspasspy.ccore.seismic import Seismogram, SeismogramEnsemble
from mspasspy.algorithms.CNRDecon import CNRRFDecon, CNRArrayDecon
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.algorithms.window import WindowData, scale
from mspasspy.algorithms.basic import ExtractComponent
from mspasspy.graphics import SeismicPlotter
import matplotlib.pyplot as plt

import decon_data_generators as simulator

wavelet = simulator.make_simulation_wavelet()
dimp = simulator.make_impulse_data()
d = simulator.convolve_wavelet(dimp, wavelet)
dwn = simulator.addnoise(d, nscale=5.0)
plotter = SeismicPlotter(scale=0.25, normalize=True)
plotter.change_style("wtva")
# plotter.title="Simulation data with noise"
# plotter.plot(dwn)
# plt.show()

pf = AntelopePf("CNRDecon.pf")
engine = CNRDeconEngine(pf)
nw = TimeWindow(-45.0, -5.0)
# useful for test but normal use would use output of broadband_snr_QC
dwn["low_f_band_edge"] = 2.0
dwn["high_f_band_edge"] = 8.0
d_decon, aout, iout = CNRRFDecon(dwn, engine, noise_window=nw, return_wavelet=True)
# d_decon = scale(d_decon,level=0.5)
# plotter.plot(d_decon)
# plt.show()
fig, ax = plt.subplots(3)
for i in range(3):
    x = ExtractComponent(d_decon, i)
    ax[i].plot(x.time_axis(), x.data)
plt.show()
fig2, ax2 = plt.subplots(2)
ax2[0].plot(aout.time_axis(), aout.data)
ax2[1].plot(iout.time_axis(), iout.data)
plt.show()

# test of array method - just make 3 copies of the same datum for the test ensemble
N = 3
e = SeismogramEnsemble()
for i in range(N):
    e.member.append(dwn)
e.set_live()
# plotter.plot(e)
# plt.show()
w = ExtractComponent(dwn, 2)
sw = TimeWindow(-5.0, 30.0)
ret = CNRArrayDecon(e, w, engine, noise_window=nw, signal_window=sw)
plotter.plot(ret)
plt.show()
