import numpy
from matplotlib import pyplot

from mspasspy.ccore.seismic import (
    Seismogram,
    TimeSeries,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
)
from mspasspy.algorithms.basic import ExtractComponent
from mspasspy.algorithms.window import scale as alg_scale


def wtva_raw(section, t0, dt, ranges=None, scale=1.0, color="k", normalize=False):
    """
    Plot a numpy 2D array (matrix) in a wiggle trace, variable area
    format as used in paper seismic section displays.   This is the low
    level function that displays a ensemble of scalar data cast into
    a matrix with the rows as time and the columns as just trace number.
    This function is called by the ensemble plotters.  For scalar data
    it is called one but for 3C data 3 windows are created with this
    function called 3 times.

    This function was taken directly from the fatiando a terra
    0.5 package.   The current web site is https://www.fatiando.org/
    and apparently the code we use here has been depricated by that
    group.   This function is unaltered from the original found on
    github for the 0.5 version.  Only the documentation was changed
    to clarify this ancestory.  We also changed the name from
    seismic_wiggle to wtva_raw to mesh more cleanly with mspass as
    the name better reflects the functions use.

    Additional changes:
        Pavlis Aug 27, 2020
    Added a test that if color was a None only the wiggles will be drawn
    (i.e. no shading)

    Parameters:

    * section :  2D array
        matrix of traces (first dimension time, second dimension traces)
    * t0 : float
        time of first sample (added for mspass - needed for passive data)
    * dt : float
        sample rate in seconds
    * ranges : (x1, x2)
        min and max horizontal values (default trace number)
    * scale : float
        scale factor multiplied by the section values before plotting
    * color : tuple of strings
        Color for filling the wiggle, positive  and negative lobes.  If None
        draw only wiggle traces
    * normalize :
        True to normalizes all trace in the section using global max/min
        data will be in the range (-0.5, 0.5) zero centered

    .. warning::
        Slow for more than 200 traces, in this case decimate your
        data or use ``image_raw``.

    """
    npts, ntraces = section.shape  # time/traces
    if ntraces < 1:
        raise IndexError("Nothing to plot")
    if npts < 1:
        raise IndexError("Nothing to plot")
    t = numpy.linspace(t0, t0 + dt * npts, npts)
    amp = 1.0  # normalization factor
    gmin = 0.0  # global minimum
    toffset = 0.0  # offset in time to make 0 centered
    if normalize:
        gmax = section.max()
        gmin = section.min()
        amp = gmax - gmin
        toffset = 0.5
    pyplot.ylim(max(t), 0)
    if ranges is None:
        ranges = (0, ntraces)
    x0, x1 = ranges
    # horizontal increment
    dx = (x1 - x0) / ntraces
    pyplot.xlim(x0 - dx / 2.0, x1 + dx / 2.0)
    for i, trace in enumerate(section.transpose()):
        tr = (((trace - gmin) / amp) - toffset) * scale * dx
        x = x0 + i * dx  # x position for this trace
        pyplot.plot(x + tr, t, "k")
        if color != None:
            pyplot.fill_betweenx(t, x + tr, x, tr > 0, color=color)


def image_raw(
    section, t0, dt, ranges=None, cmap=pyplot.cm.gray, aspect=None, vmin=None, vmax=None
):
    """
    Plot a numpy 2D array (matrix) in an image format.  This type of
    plot has two uses: (1) large numbers of spatially coherent signals
    (as in a conventional seismic reflection) are more clearly displayed
    this way than as wiggles, and (2) this function can be called
    immediately following a call to wtva_raw to produce an section
    with variable area wiggle trace data shown on top of the an image
    representation of the data data (note for dense data it is usually
    necessary to decimate the data on the column axis to make a
    clean plot - functions to make such plots need a decimate parameter).
    This is the low level function to make such plots.  It is
    directly comparable to wtva_raw.

    This function was taken directly from the fatiando a terra
    0.5 package.   The current web site is https://www.fatiando.org/
    and apparently the code we use here has been depricated by that
    group.   This function is unaltered from the original found on
    github for the 0.5 version.  Only the documentation was changed
    to clarify this ancestory.  We also changed the name from
    seismic_image to image_raw to mesh more cleanly with mspass as
    the name better reflects the functions use.

    Parameters:

    * section :  2D array
        matrix of traces (first dimension time, second dimension traces)
    * t0 : float
        time of first sample (added for mspass - needed for passive data)
    * dt : float
        sample rate in seconds
    * ranges : (x1, x2)
        min and max horizontal coordinate values (default trace number)
    * cmap : colormap
        color map to be used. (see pyplot.cm module)
    * aspect : float
        matplotlib imshow aspect parameter, ratio between axes
    * vmin, vmax : float
        min and max values for imshow

    """
    npts, maxtraces = section.shape  # time/traces
    if maxtraces < 1:
        raise IndexError("Nothing to plot")
    if npts < 1:
        raise IndexError("Nothing to plot")
    t = numpy.linspace(t0, t0 + dt * npts, npts)
    data = section
    if ranges is None:
        ranges = (0, maxtraces - 1)
    x0, x1 = ranges
    extent = (x0 - 0.5, x1 + 0.5, t[npts - 1], t[0])
    if aspect is None:  # guarantee a rectangular picture
        aspect = numpy.round((x1 - x0) / numpy.max(t))
        if aspect <= 0.0:
            aspect = 1.0
        aspect -= aspect * 0.2
    if vmin is None and vmax is None:
        scale = numpy.abs([section.max(), section.min()]).max()
        vmin = -scale
        vmax = scale
    pyplot.imshow(
        data,
        aspect=aspect,
        cmap=cmap,
        origin="upper",
        extent=extent,
        vmin=vmin,
        vmax=vmax,
    )


def tse2nparray(ens):
    nseis = len(ens.member)
    tmax = 0.0
    tmin = 0.0
    dt = 0.0
    for i in range(nseis):
        if i == 0:
            tmin = ens.member[i].t0
            tmax = ens.member[i].endtime()
            dt = ens.member[i].dt
        else:
            tmin = min(tmin, ens.member[i].t0)
            tmax = max(tmax, ens.member[i].endtime())
            # check for irregular sample rates.  Test uses a fractional
            # tolerance that is a frozen constant here
            delta_dt = abs(dt - ens.member[i].dt)
            if delta_dt / dt > 0.01:
                raise RuntimeError(
                    "tse2dmatrix:  Irregular sample rates - cannot convert"
                )
    # A naive user might pass an ensemble of data with absolute times
    # spanning years.  The calculation here uses a threshold on size
    # as a sanity check to avoid absurd malloc requests
    n = nseis
    m = int((tmax - tmin) / dt + 1)
    Mmax = 10000000  # size limit hard wired
    if m > Mmax:
        raise RuntimeError(
            "tse2dmatix:  irrational computed time range - you are probably incorrectly using data with large range of absolute times"
        )
    work = numpy.zeros(shape=[m, n])
    # The algorithm used here is horribly inefficient if there are large
    # differences in start and end times but this approach is safer
    for j in range(n):
        tjstart = ens.member[j].t0
        tjend = ens.member[j].endtime()
        for i in range(m):
            t = tmin + i * dt
            if (t >= tjstart) and (t <= tjend):
                k = ens.member[j].sample_number(t)
                work[i, j] = ens.member[j].data[k]
    return [tmin, dt, work]


def seis2nparray(d):
    tmin = d.t0
    dt = d.dt
    m = d.npts
    n = 3
    work = numpy.array(d.data)
    return [tmin, dt, work]


def ts2nparray(d):
    tmin = d.t0
    dt = d.dt
    m = d.npts
    work = numpy.array(d.data)
    return [tmin, dt, work]


def wtvaplot(
    d, ranges=None, scale=1.0, fill_color="k", normalize=False, cmap=None, title=None
):
    """
    Wiggle trace variable area plotter for mspass ensemble objects.
    """
    # We have to handle 3C ensembles specially to make 3 separate
    # windows.   this logic is potentially confusing.  the else
    # block handles all but 3C ensembles - common read structure
    # makes a single plot call work for all 3 cases
    figure_handles = []
    if isinstance(d, SeismogramEnsemble):
        # We always plot 3C data as 3 windows.  We extract each
        # component and then call this function with a trivial
        # recursion - only call itself once and only once
        title3c = title
        for i in range(3):
            pyplot.figure(i)
            dcomp = ExtractComponent(d, i)
            if title != None:
                title3c = "%s:%d" % (title, i)
            try:
                [t0, dt, section] = tse2nparray(dcomp)
                wtva_raw(section, t0, dt, ranges, scale, fill_color, normalize)
                if cmap != None:
                    image_raw(section, t0, dt, ranges, cmap)
                if title3c != None:
                    pyplot.title(title3c)
                figure_handles.append(pyplot.gcf())
            except RuntimeError as err:
                print(err)
                return None
    else:
        try:
            # need to force these into the scope of this block
            plotdata = []
            if isinstance(d, TimeSeriesEnsemble):
                plotdata = tse2nparray(d)
            elif isinstance(d, Seismogram):
                plotdata = seis2nparray(d)
            elif isinstance(d, TimeSeries):
                plotdata = ts2nparray(d)
            else:
                raise RuntimeError(
                    "wtvaplot - data received is not one supported by mspass"
                )
            t0 = plotdata[0]
            dt = plotdata[1]
            section = plotdata[2]
            wtva_raw(section, t0, dt, ranges, scale, fill_color, normalize)
            if cmap != None:
                image_raw(section, t0, dt, ranges, cmap)
            if title != None:
                pyplot.title(title)
            figure_handles.append(pyplot.gcf())
        except RuntimeError as err:
            print(err)
            return None
    return figure_handles


def imageplot(
    d, ranges=None, cmap=pyplot.cm.gray, aspect=None, vmin=None, vmax=None, title=None
):
    """
    Image plotter for mspass ensemble objects.
    """
    # We have to handle 3C ensembles specially to make 3 separate
    # windows.   this logic is potentially confusing.  the else
    # block handles all but 3C ensembles - common read structure
    # makes a single plot call work for all 3 cases
    figure_handles = []
    if isinstance(d, SeismogramEnsemble):
        # We always plot 3C data as 3 windows.  We extract each
        # component and then call this function with a trivial
        # recursion - only call itself once and only once
        title3c = title
        for i in range(3):
            pyplot.figure(i)
            dcomp = ExtractComponent(d, i)
            if title != None:
                title3c = "%s:%d" % (title, i)
            try:
                [t0, dt, section] = tse2nparray(dcomp)
                image_raw(section, t0, dt, ranges, cmap, aspect, vmin, vmax)
                if title3c != None:
                    pyplot.title(title3c)
                figure_handles.append(pyplot.gcf())
            except RuntimeError as err:
                print(err)
                return None
    else:
        try:
            # need to force these into the scope of this block
            plotdata = []
            if isinstance(d, TimeSeriesEnsemble):
                plotdata = tse2nparray(d)
            elif isinstance(d, Seismogram):
                plotdata = seis2nparray(d)
            elif isinstance(d, TimeSeries):
                plotdata = ts2nparray(d)
            else:
                raise RuntimeError(
                    "wtvaplot - data received is not one supported by mspass"
                )
            t0 = plotdata[0]
            dt = plotdata[1]
            section = plotdata[2]
            image_raw(section, t0, dt, ranges, cmap, aspect, vmin, vmax)
            if title != None:
                pyplot.title(title)
            figure_handles.append(pyplot.gcf())
        except RuntimeError as err:
            print(err)
            return None
    return figure_handles


class SectionPlotter:
    """
    This class was designed to plot data in a seismic reflection style
    display.  That means time is the y axis but runs in a reverse direction
    and traces are equally spaced along the x axis. The data can be plotted
    as simple wiggle traces, wiggle trace variable area, an image format,
    and a wiggletrace variable area plot overlaying an image plot.

    The object itself only defines the style of plot to be produced along
    with other assorted plot parameters.   The plot method can be called on
    any mspass data object to produce a graphic display.   TimeSeries
    data produce a frame with one seismogram plotted.  Seismogram data
    will produce a one frame display with the 3 components arranged in
    component order from left to right.  TimeSeriesEnsembles produce
    conventional reflection style sections with the data displayed from
    left to right in whatever order the ensemble is sorted to.
    SeismogramEnsembles are the only type that create multiple windows.
    That is, each component is displayed in a different window.   The
    display can be understood as a conversion of a SeismogramEnsemble to
    a set of 3 TimeSeriesEnsembles that are displayed in 3 windows.
    The component can be deduced only from the title string with
    has :0, :1, and :2 appended to it to denote components 0, 1, and 2
    respectively.

    The plots produced by this object are simple ones for display only.
    The concept is that fancier tools for gui interaction like a trace
    editor be created as children of this base class.
    """

    def __init__(self, scale=1.0, normalize=False, title=None):
        """
        The constructor initializes all the internal variables that define
        the possible behaviors of the plotting engine.  The constructor
        currently does no sanity check on the compatibility of the parameters.
        The intent is overall behavior should be altered from the default
        only through the change_style method.   The public attributes
        (scale,normalize, and title) can be changed without hard.  Those
        defined as private should be altered only indirectly through the
        change_style method.

        :param scale:  Set the amount by which the data should be scaled
          before plotting.   Note the assumption is the data amplitude is
          of order 1.  Use normalize if that assumption is bad.
        :param normalize:   boolean used to determine if data need to be
          autoscaled. If true a section scaling is produced from the minimum
          and maximum of the input data.  Relative amplitudes between
          each scalar signal will be preserved.  To change scaling use
          a mspass amplitude scaling function before calling the plotter.
        :param title: is a string written verbatim at the top of each
          plot.  For 3C ensembles the string :0, :1, and :2 is added to
          title for each of the 3 windows drawn for each of the 3 components
          of the parent ensemble.
        """
        # these are public because they can set independently
        self.scale = scale
        self.title = title
        self.normalize = normalize
        # these have some interdependencies and are best altered only
        # through the change_style method.  This may be unnecessary
        # as it may duplicate the call to change_style at the end, BUT
        # if the default changes these initial values do not need to be
        # changed
        self._style = "wtvaimg"
        self._use_variable_area = True
        self._fill_color = "k"  # black in matplotlib
        self._color_background = True
        self._color_map = "seismic"
        # these are options to raw codes adapted from  fatiando a terra
        # that are currently ignored.   Convert to args for __init__ if
        # it proves useful to have them in the api
        self._ranges = None
        self._aspect = None
        self._vmin = None
        self._vmax = None
        # use change_style to simply default style
        self.change_style("wtvaimg")

    def change_style(self, newstyle, fill_color="k", color_map="seismic"):
        """
        Use this method to change the plot style.   Options are described
        below.   Some parameter combinations are illegal and will result in
        a RuntimeError if used.  The illegal combos are listed below for
        each style.

        :param newstyle:  This is the primary switch for this method and
          is used to define the style of plot that will be produced. The
          allowed values for this string are listed below along with
          restrictions (when they exist) on related parameters.

          wtva - wiggle trace variable area.  That means a wiggle traced with
            the positive quadrants colored by the color defined by the fill_color
            parameter.   If fill_color is passed as None a RuntimeError will
            be thrown.
          img - data are displayed as an image with the color_map parameter
            defining the matplotlib color map used to map amplitudes into
            a given color.   Note as with wtva the data are presumed to be
            scaled to be of order 1.  color_map must not be None, which it
            can be if the plot was changed from a previous style like wtva,
            or a RuntimeError exception will be thrown.
          wtvaimg is like wtva but the wtva plot is drawn on top of an
            image plot.  Since this is the union of wtva and img a RuntimeError
            exception will be throw if either color_map or fill_color are null.
          wt will produce a standard line graph of each input as a
            black line (that feature is currently frozen).   color_map and
            fill_color are ignored for this style so no exceptions should
            occur when the method is called with this value of newstyle.
        """
        if newstyle == "wtva":
            if fill_color == None:
                raise RuntimeError(
                    "SectionPlotter.change_style: wtva style requires fill_color to be a valid color code - received a None"
                )
            self._style = "wtva"
            self._color_background = False
            # force this when set this way
            self._color_map = None
            self._fill_color = fill_color
            self._use_variable_area = True
        elif newstyle == "wtvaimg":
            if color_map == None:
                raise RuntimeError(
                    "SectionPlotter.change_style: wtvaimg style requires a color_map definition - received a None"
                )
            if fill_color == None:
                raise RuntimeError(
                    "SectionPlotter.change_style: wtvaimg style requires a fill_color definition - received a None"
                )
            self._style = "wtvaimg"
            self._color_background = True
            self._fill_color = fill_color
            self._color_map = color_map
            self._use_variable_area = True
        elif newstyle == "img":
            if color_map == None:
                raise RuntimeError(
                    "SectionPlotter.change_style: img style requires a color_map definition - received a None"
                )
            self._style = "img"
            self._color_background = True
            self._fill_color = None
            self._use_variable_area = False
            self._color_map = color_map
        elif newstyle == "wt":
            self._style = "wt"
            self._color_background = False
            self._color_map = None
            self._fill_color = None
            self._use_variable_area = False
        else:
            raise RuntimeError(
                "SectionPlotter.change_style:  unknown style type=" + newstyle
            )

    def plot(self, d):
        """
        Call this method to plot any data using the current style setup and any
        details defined by public attributes.

        :param d:  is the data to be plotted.   It can be any of the following:
            TimeSeries, Seismogram, TimeSeriesEnsemble, or SeismogramEnsemble.
            If d is any other type the method will throw a RuntimeError exception.

        :Returns: an array of one or 3 (only for SeismogramEnsemble dat)
          matplotlib.pylot.gcf() plot handle(s).
        :rtype: The plot handle is what matplotlib.pyplot calls a gcf.  It
          is the return of pyplot.gcf() and can be used to alter some properties
          of the figure.  See matplotlib documentation.
        """
        # these are all handled by the same function with argument combinations defined by
        # change_style determining the behavior.
        if self._style == "wtva" or self._style == "wtvaimg" or self._style == "wt":
            handle = wtvaplot(
                d,
                self._ranges,
                self.scale,
                self._fill_color,
                self.normalize,
                self._color_map,
                self.title,
            )
            return handle
        elif self._style == "img":
            handle = imageplot(
                d,
                self._ranges,
                self._color_map,
                self._aspect,
                self._vmin,
                self._vmax,
                self.title,
            )
            return handle
        else:
            raise RuntimeError(
                "SectionPlotter.plot:  internal style definition="
                + self._style
                + " which is illegal.  Run change_style method"
            )


class SeismicPlotter:
    """
    SeismicPlotter is used to plot mspass data objects in the
    seismology convention with time as the x axis.  Use SectionPlotter
    to plot data in the seismic reflection convention with time on the
    y axis and oriented backward (increasing downward).

    Four types of plots are supported:  wiggle trace, wiggle trace variable
    area (i.e. filled on positive peaks), image plots, and colored wiggle
    trace variable area (wtva overlays an image display).
    Ensembles of Seismogram objects (SeismogramEnsemble object) are plotted in
    three windows that will appear as Figure 0, Figure 1, and Figure 2.
    All other object are displayed in a single window.   Default display
    order for ensembles is fill in ensemble order from the bottom up.
    Use the topdown() method to switch to reverse order.

    This is a simple plotter that just generates a basic graphic.
    The design idea is to use this as a base class for more advanced
    graphics uses like a trace editor or generic picker.  The
    plot object must be created before any plots can be generated.
    Any optional changes to the plot behavior, most commonly calls
    to the change_style method, should be done before calling plot.
    """

    def __init__(self, scale=1.0, normalize=False, title=None):
        """
        Constructor for this object.   It mostly sets defaults but
        has a few common optional parameters to set at construction
        time.  Note style is intentionally not a constructor
        parameter because of parameter interdependence.  The default
        plot style is "wtva".  Use change_style to use change plot
        style.

        :param scale:  optional scale factor to apply to data before plotting
          (default assumes data have been scaled to amplitude of order 1)
        :param  normalize:  Default assumes the data have been scaled with
        one of the mspass amplitude scaling functions to be of order one
        (or the scale parameter).  Set True if the data are to be
        normalized internally by the plotter.
        :param title:   optional title to put on graphics.  Note for
        SeismogramEnsembles each of the three windows will be tagged
        with a variant of this title string adding ":n" where n is
        the component number (0,1,2).
        """
        # these are public because they can set independently
        self.scale = scale
        self.title = title
        self.normalize = normalize
        # these have some interdependencies and are best altered only
        # through the change_style method.  This may be unnecessary
        # as it may duplicate the call to change_style at the end, BUT
        # if the default changes these initial values do not need to be
        # changed
        self._style = "wtva"
        self._fill_color = "k"  # black in matplotlib
        self._color_map = "seismic"
        # these are options to raw codes adapted from  fatiando a terra
        # that are currently ignored.   Convert to args for __init__ if
        # it proves useful to have them in the api
        self._ranges = None
        self._aspect = None
        self._vmin = None
        self._vmax = None
        # These are added for SeismicPlotter and not in SectionPlotter
        self._plot_topdown = False
        # Internal constants
        # Should be smaller than 1/screem horizontal pixel maximum size
        self._RANGE_RATIO_TEST = 0.0001
        self._default_single_ts_aspect = 0.25
        # use change_style to simply default style
        self.change_style("wtvaimg")

    def change_style(self, newstyle, fill_color="k", color_map="seismic"):
        """
        Use this method to change the plot style.   Options are described
        below.   Some parameter combinations are illegal and will result in
        a RuntimeError if used.  The illegal combos are listed below for
        each style.

        :param newstyle:  This is the primary switch for this method and
          is used to define the style of plot that will be produced. The
          allowed values for this string are listed below along with
          restrictions (when they exist) on related parameters.

          wtva - wiggle trace variable area.  That means a wiggle traced with
            the positive quadrants colored by the color defined by the fill_color
            parameter.   If fill_color is passed as None a RuntimeError will
            be thrown.
          img - data are displayed as an image with the color_map parameter
            defining the matplotlib color map used to map amplitudes into
            a given color.   Note as with wtva the data are presumed to be
            scaled to be of order 1.  color_map must not be None, which it
            can be if the plot was changed from a previous style like wtva,
            or a RuntimeError exception will be thrown.
          wtvaimg is like wtva but the wtva plot is drawn on top of an
            image plot.  Since this is the union of wtva and img a RuntimeError
            exception will be throw if either color_map or fill_color are null.
          wt will produce a standard line graph of each input as a
            black line (that feature is currently frozen).   color_map and
            fill_color are ignored for this style so no exceptions should
            occur when the method is called with this value of newstyle.
        """
        if newstyle == "wtva":
            if fill_color == None:
                raise RuntimeError(
                    "SectionPlotter.change_style: wtva style requires fill_color to be a valid color code - received a None"
                )
            self._style = "wtva"
            # force this when set this way
            self._color_map = None
            self._fill_color = fill_color
        elif newstyle == "wtvaimg":
            if color_map == None:
                raise RuntimeError(
                    "SectionPlotter.change_style: wtvaimg style requires a color_map definition - received a None"
                )
            if fill_color == None:
                raise RuntimeError(
                    "SectionPlotter.change_style: wtvaimg style requires a fill_color definition - received a None"
                )
            self._style = "wtvaimg"
            self._fill_color = fill_color
            self._color_map = color_map
        elif newstyle == "img":
            if color_map == None:
                raise RuntimeError(
                    "SectionPlotter.change_style: img style requires a color_map definition - received a None"
                )
            self._style = "img"
            self._fill_color = None
            self._color_map = color_map
        elif newstyle == "wt":
            self._style = "wt"
            self._color_map = None
            self._fill_color = None
        else:
            raise RuntimeError(
                "SectionPlotter.change_style:  unknown style type=" + newstyle
            )

    # These two method should perhaps be implemented as decorators

    def topdown(self):
        """
        Switch to mode of plotting ensembles from the top downward
        from default of bottum upward.
        """
        self._plot_topdown = True

    def bottomup(self):
        """
        Restore (or force) default ensemble plot order of bottup upward.
        """
        self._plot_topdown = False

    def plot(self, d):
        # make copy always to prevent unintentional scaling of input data
        if self.normalize:
            d2plot = self._deepcopy(d)
            self._normalize(d2plot)
        else:
            d2plot = d  # always a shallow copy in python
        if self._style == "wtva":
            self._wtva(d2plot, fill=True)
        elif self._style == "wt":
            self._wtva(d2plot, fill=False)
        elif self._style == "wtvaimg":
            self._wtva(d2plot, fill=True)
            self._imageplot(d2plot)
        elif self._style == "img":
            self._imageplot(d2plot)
        else:
            raise RuntimeError(
                "SeismicPlotter.plot:  internal style definition="
                + self._style
                + " is invalid\nThis should not happen"
            )

    def _deepcopy(self, d):
        """
        Private helper method for immediately above.   Necessary because
        copy.deepcopy doesn't work with our pybind11 wrappers. There may be a
        fix, but for now we have to use copy constructors specific to each
        object type.
        """
        if isinstance(d, TimeSeries):
            return TimeSeries(d)
        elif isinstance(d, Seismogram):
            return Seismogram(d)
        elif isinstance(d, TimeSeriesEnsemble):
            return TimeSeriesEnsemble(d)
        elif isinstance(d, SeismogramEnsemble):
            return SeismogramEnsemble(d)
        else:
            raise RuntimeError(
                "SeismicPlotter._deepcopy:  received and unsupported data type=",
                type(d),
            )

    def set_topdown(self):
        """
        Call this method to have datat plotted from top down.  Default plots
        ensemble member 0 at the bottom of the plot with successive members
        plotted in units of 1.0 above the last.  When this is called the
        order is reversed.
        """
        self._plot_topdown = True

    # These are private methods used internally

    def _normalize(self, d):
        """
        Normalizes data (d) using scale function.  for ensembles that
        is peak normalization to level self.scale by section. For
        Seismograms each component will be normalized independently.
        For TimeSeries the peak is adjusted to self.scale.

        :param d:  input data to be scanned (must be one of mspass supported
        data objects or will throw an exception)
        :param perf:  clip level as used in seismic unix displays.
        """
        # These are place holders for now.  Requires some new code in ccore
        # to sort absolute values and return perf level - should use faster
        # max value when perf is 100%
        if isinstance(d, SeismogramEnsemble):
            alg_scale(d, scale_by_section=True, level=self.scale)
        elif isinstance(d, TimeSeriesEnsemble):
            alg_scale(d, scale_by_section=True, level=self.scale)
        elif isinstance(d, TimeSeries):
            alg_scale(d, level=self.scale)
        elif isinstance(d, Seismogram):
            alg_scale(d, level=self.scale)
        else:
            raise RuntimeError(
                "SeismicPlotter._normalize:  Received unsupported data type=", type(d)
            )

    def _add_3C_titles(self):
        """
        Private method to add titles with pyplot.title to 3C ensemble data.
        This algorithm is a bit fragile as it depends upon use of
        figure tagged with integer component number.
        """
        if self.title != None:
            for k in range(3):
                pyplot.figure(k)
                title3c = "%s:%d" % (self.title, k)
                pyplot.title(title3c)

    def _wtva(self, d, fill=True):
        """
        Private method for making all forms of wiggle trace plots.  The
        fill boolean determines if shading is applied.  When used the shading
        is assumed properly defined by self._fill_color.   In this implementation
        this function does little more than determine the type of the input
        data d and call the appropriate private method for that data type.
        """
        base_error = "SeismicPlotter._imageplot (Error):  "
        if isinstance(d, SeismogramEnsemble):
            if len(d.member) <= 0:
                raise IndexError(base_error + "ensemble container is empty")
            self._wtva_SeismogramEnsemble(d, fill)
            self._add_3C_titles()
        elif isinstance(d, TimeSeriesEnsemble):
            if len(d.member) <= 0:
                raise IndexError(base_error + "ensemble container is empty")
            self._wtva_TimeSeriesEnsemble(d, fill)
            if self.title != None:
                pyplot.title(self.title)
        elif isinstance(d, TimeSeries):
            if d.npts <= 0:
                raise IndexError(base_error + "data vector is empty.  Nothing to plot")
            self._wtva_TimeSeries(d, fill)
            if self.title != None:
                pyplot.title(self.title)
        elif isinstance(d, Seismogram):
            if d.npts <= 0:
                raise IndexError(base_error + "data vector is empty.  Nothing to plot")
            self._wtva_Seismogram(d, fill)
            if self.title != None:
                pyplot.title(self.title)
        else:
            raise RuntimeError(base_error + "Received unsupported data type=", type(d))

    def _imageplot(self, d):
        """
        Private method for making all forms of image plots.
        """
        base_error = "SeismicPlotter._imageplot (Error):  "
        if isinstance(d, SeismogramEnsemble):
            if len(d.member) <= 0:
                raise IndexError(base_error + "ensemble container is empty")
            self._imageplot_SeismogramEnsemble(d)
            self._add_3C_titles()
        elif isinstance(d, TimeSeriesEnsemble):
            if len(d.member) <= 0:
                raise IndexError(base_error + "ensemble container is empty")
            self._imageplot_TimeSeriesEnsemble(d)
            if self.title != None:
                pyplot.title(self.title)
        elif isinstance(d, TimeSeries):
            if d.npts <= 0:
                raise IndexError(base_error + "data vector is empty.  Nothing to plot")
            self._imageplot_TimeSeries(d)
            if self.title != None:
                pyplot.title(self.title)
        elif isinstance(d, Seismogram):
            if d.npts <= 0:
                raise IndexError(base_error + "data vector is empty.  Nothing to plot")
            self._imageplot_Seismogram(d)
            if self.title != None:
                pyplot.title(self.title)
        else:
            raise RuntimeError(base_error + "Received unsupported data type=", type(d))

    def _wtva_TimeSeries(self, d, fill):
        # this plot reduces to a simple call to plot defining a time axis from
        # a single trace - pretty much like the obspy plot but with optional
        # shading.  It assumes d is a TimeSeries.

        t = numpy.linspace(d.t0, d.t0 + d.dt * d.npts, d.npts)
        # We don't need a gain factor here - will be needed for an image overlay through
        pyplot.plot(t, d.data, "k")
        if fill:
            # Necessary because fill_between doesn't support the pybind11
            # wrapped vector directly - need to convert to numpy array
            y = numpy.array(d.data)
            pyplot.fill_between(
                t, 0, y, where=y > 0.0, interpolate=True, color=self._fill_color
            )
        return pyplot.gcf()

    def _wtva_Seismogram(self, d, fill):
        # this could be implemented by converting d to an ensemble
        ens = TimeSeriesEnsemble()
        for k in range(3):
            dcomp = ExtractComponent(d, k)
            ens.member.append(dcomp)
        self._wtva_TimeSeriesEnsemble(ens, fill)

    def _get_ensemble_size(self, d):
        # Unlike SectionPlotter we allow irregular start times and don't
        # need to do the baggage of zero padding - will be needed for image
        # plots though.  So, step one is to scan for the time range.  We
        # also search for longest time interval and throw and exception if
        # the ratio of that time interval to data time interval span is too
        # small.
        moderror = "SeismicPlotter._get_ensemble_size (Error):  "
        ndata = len(d.member)
        if ndata <= 0:
            raise RuntimeError("Trying to plot an empty ensemble")
        t0 = []
        endtimes = []
        nlive = 0
        for i in range(ndata):
            if d.member[i].live:
                t0.append(d.member[i].t0)
                endtimes.append(d.member[i].endtime())
                nlive += 1
        if nlive == 0:
            raise RuntimeError(
                moderror + "All members of ensemble are marked dead - nothing to plot"
            )
        tmin = min(t0)
        tmax = max(endtimes)
        if (tmax - tmin) <= 0:
            raise RuntimeError(moderror + "time span of data is zero")
        maxdt = 0.0
        # this is no doubt a faster vector way to do this calculation, but
        # this loop is guaranteed by screen limits to never be huge
        for i in range(len(t0)):
            dt = endtimes[i] - t0[i]
            maxdt = max(maxdt, dt)
        if maxdt / (tmax - tmin) < self._RANGE_RATIO_TEST:
            raise RuntimeError(
                moderror
                + "data appear to be mix of data with absolute times.  Computed time range is absurd - convert data to relative time"
            )
        return (ndata, tmin, tmax)

    def _wtva_TimeSeriesEnsemble(self, d, fill):
        (ndata, tmin, tmax) = self._get_ensemble_size(d)
        pyplot.xlim(tmin, tmax)
        # This plotting engine always equally spaces traces horizontally so
        # unlike SectionPlotter we just compute the range as the number of
        # intervals + 1 for padding
        pyplot.ylim(-0.5, float(ndata) - 0.5)
        for i in range(ndata):
            # skip data marked dead - this will leave a hole in plot.  We could
            # plot a line but this is proably better unless proven otherwise
            if d.member[i].dead():
                continue
            # Make a copy - fast method with numpy
            y = numpy.array(d.member[i].data)
            # Fast and easy way to add offset with overloaded operator -= and +=
            if self._plot_topdown:
                offset = ndata - i - 1
                y += offset
            else:
                y += i
                offset = i
            t0 = d.member[i].t0
            endtime = d.member[i].endtime()
            npts = d.member[i].npts
            t = numpy.linspace(t0, endtime, npts)
            pyplot.plot(t, y, "k")
            if fill:
                pyplot.fill_between(
                    t,
                    offset,
                    y,
                    where=y > offset,
                    interpolate=True,
                    color=self._fill_color,
                )
        return pyplot.gcf()

    def _wtva_SeismogramEnsemble(self, d, fill):
        # implement by call to ExtractComponent and calling TimeSeriesEnsemble method 3 times
        # should return a list of 3 gcf handles
        figure_handles = []
        for k in range(3):
            dcomp = ExtractComponent(d, k)
            # figure_title='Component %d' % k
            # pyplot.figure(figure_title)
            pyplot.figure(k)
            handle = self._wtva(dcomp)
            figure_handles.append(handle)
        # pyplot.show()
        return figure_handles

    def _imageplot_TimeSeriesEnsemble(self, d):
        (ndata, tmin, tmax) = self._get_ensemble_size(d)
        extent = (tmin, tmax, -0.5, float(ndata) - 0.5)
        # left off here - below is copy from SectionPlotter
        # need a different algorithm to support mixed sample
        # rates.  Probably should search for shortest dt and
        # define the matrix from that

        # to handle mixed sample rate data use the shortest dt and
        # use a crude boxcar resampling for data with larger dt
        # Boxcar resampling is implict in use of time method which gets the
        # nearest sample
        dt = 10000000.0
        for i in range(ndata):
            dt = min(dt, d.member[i].dt)
        # compute the number of points for the time axis
        nt = int((tmax - tmin) / dt) + 1
        # WARNING - this assumes size of nt is limited by error checking
        # in get_ensemble_size called above.  Prone to malloc error if nt
        # is huge - easy to do with absolute time data segments
        work = numpy.zeros(shape=[ndata, nt])
        for i in range(ndata):
            # skip data marked dead
            if d.member[i].dead():
                continue
            t0 = d.member[i].t0
            endtime = d.member[i].endtime()
            npts = d.member[i].npts
            if self._plot_topdown:
                iwork = ndata - i - 1
            else:
                iwork = i
            if t0 == tmin:
                t = tmin
                j = 0
            else:  # above logic guarantees < not possible so this is > block
                t = t0
                # often one less than needed but minimal inefficiency
                j = int((t0 - tmin) / dt)
            # tmax test shouldn't be necessary but small cost for safety
            while t <= endtime and t <= tmax:
                k = d.member[i].sample_number(t)
                if k > 0 and k < npts:
                    work[iwork, j] = d.member[i].data[k]
                t += dt
                j += 1
        if self._aspect is None:  # guarantee a rectangular picture
            aspect = (tmax - tmin) / ndata
            # avoid long, skinny plots that would be the norm from
            # above calculation
            if aspect > 10.0:
                aspect = 0.8
        else:
            aspect = self._aspect
        if self._vmin is None and self._vmax is None:
            scale = numpy.abs([work.max(), work.min()]).max()
            vmin = -scale
            vmax = scale
        # imshow handles topdown or updown order with this parameter
        if self._plot_topdown:
            origin_position = "upper"
            extent = (tmin, tmax, float(ndata) - 0.5, -0.5)
        else:
            origin_position = "lower"
        pyplot.imshow(
            work,
            aspect="auto",
            cmap=self._color_map,
            origin=origin_position,
            extent=extent,
            vmin=vmin,
            vmax=vmax,
        )

    def _imageplot_SeismogramEnsemble(self, d):
        # implement by call to ExtractComponent and calling TimeSeriesEnsemble method 3 times
        # should return a list of 3 gcf handles
        figure_handles = []
        for k in range(3):
            dcomp = ExtractComponent(d, k)
            pyplot.figure(k)
            figure = self._imageplot(dcomp)
            figure_handles.append(figure)
        # pyplot.show()
        return figure_handles

    def _imageplot_Seismogram(self, d):
        # this could be implemented by converting d to an ensemble
        ens = TimeSeriesEnsemble()
        for k in range(3):
            dcomp = ExtractComponent(d, k)
            ens.member.append(dcomp)
        self._imageplot_TimeSeriesEnsemble(ens)

    def _imageplot_TimeSeries(self, d):
        # this plot reduces to a simple call to plot defining a time axis from
        # a single trace - pretty much like the obspy plot but with optional
        # shading.  It assumes d is a TimeSeries.

        t = numpy.linspace(d.t0, d.endtime(), d.npts)
        # somewhat inefficient, but TimeSeries size in this context
        # would always be small enough to be irrelevant
        work = numpy.zeros(shape=[1, d.npts])
        extent = (d.t0, d.endtime(), -1.0, 1.0)
        for j in range(d.npts):
            work[0, j] = d.data[j]
        if self._aspect == None:
            aspect = self._default_single_ts_aspect
        if self._vmin is None and self._vmax is None:
            scale = numpy.abs([work.max(), work.min()]).max()
            vmin = -scale
            vmax = scale
        pyplot.imshow(
            work,
            aspect="auto",
            cmap=self._color_map,
            extent=extent,
            vmin=vmin,
            vmax=vmax,
        )
        return pyplot.gcf()
