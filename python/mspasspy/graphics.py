import numpy
from matplotlib import pyplot
# this obnoxious thing is needed for testing for now
import sys
sys.path.append('/home/pavlis/src/mspass/python')
import mspasspy.ccore as mspass
from mspasspy.ccore import TimeSeriesEnsemble
from mspasspy.ccore import SeismogramEnsemble


def wtva_raw(section, t0, dt, ranges=None, scale=1., color='k',
                   normalize=False):
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
    t = numpy.linspace(t0, t0+dt*npts, npts)
    amp = 1.  # normalization factor
    gmin = 0.  # global minimum
    toffset = 0.  # offset in time to make 0 centered
    if normalize:
        gmax = section.max()
        gmin = section.min()
        amp = (gmax - gmin)
        toffset = 0.5
    pyplot.ylim(max(t), 0)
    if ranges is None:
        ranges = (0, ntraces)
    x0, x1 = ranges
    # horizontal increment
    dx = (x1 - x0)/ntraces
    pyplot.xlim(x0-dx/2.0, x1+dx/2.0)
    for i, trace in enumerate(section.transpose()):
        tr = (((trace - gmin)/amp) - toffset)*scale*dx
        x = x0 + i*dx  # x position for this trace
        pyplot.plot(x + tr, t, 'k')
        if(color!=None):
            pyplot.fill_betweenx(t, x + tr, x, tr > 0, color=color)


def image_raw(section, t0, dt, ranges=None, cmap=pyplot.cm.gray,
                  aspect=None, vmin=None, vmax=None):
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
    t = numpy.linspace(t0, t0+dt*npts, npts)
    data = section
    if ranges is None:
        ranges = (0, maxtraces)
    x0, x1 = ranges
    extent = (x0, x1, t[-1:], t[0])
    if aspect is None:  # guarantee a rectangular picture
        aspect = numpy.round((x1 - x0)/numpy.max(t))
        if(aspect<=0.0):
            aspect=1.0
        aspect -= aspect*0.2
    if vmin is None and vmax is None:
        scale = numpy.abs([section.max(), section.min()]).max()
        vmin = -scale
        vmax = scale
    pyplot.imshow(data, aspect=aspect, cmap=cmap, origin='upper',
                  extent=extent, vmin=vmin, vmax=vmax)
  
def tse2nparray(ens):
    nseis=len(ens.member)
    tmax=0.0
    tmin=0.0
    dt=0.0
    for i in range(nseis):
        if(i==0):
            tmin=ens.member[i].t0
            tmax=ens.member[i].endtime()
            dt=ens.member[i].dt
        else:
            tmin=min(tmin,ens.member[i].t0)
            tmax=max(tmax,ens.member[i].endtime())
            # check for irregular sample rates.  Test uses a fractional 
            # tolerance that is a frozen constant here
            delta_dt=abs(dt-ens.member[i].dt)
            if( delta_dt/dt > 0.01):
                raise RuntimeError("tse2dmatrix:  Irregular sample rates - cannot convert")
    #A naive user might pass an ensemble of data with absolute times 
    #spanning years.  The calculation here uses a threshold on size
    # as a sanity check to avoid absurd malloc requests 
    n=nseis
    m=int((tmax-tmin)/dt + 1)
    Mmax=10000000   # size limit hard wired
    if(m>Mmax):
        raise RuntimeError("tse2dmatix:  irrational computed time range - you are probably incorrectly using data with large range of absolute times")
    work=numpy.zeros(shape=[m,n])
    # The algorithm used here is horribly inefficient if there are large
    # differences in start and end times but this approach is safer
    for j in range(n):
        tjstart=ens.member[j].t0
        tjend=ens.member[j].endtime()
        for i in range(m):
            t=tmin+i*dt
            if( (t >= tjstart) and (t <= tjend) ):
                k=ens.member[j].sample_number(t)
                work[i,j]=ens.member[j].s[k]
    return [tmin,dt,work]
def seis2nparray(d):
    tmin=d.t0
    dt=d.dt
    m=d.npts
    n=3
    work=numpy.zeros(shape=[m,n])
    # this is a very slow way to do a matrix transpose of u dmatrix
    for k in range(n):
        for j in range(m):
            work[j,k]=d.u[k,j]
    return [tmin,dt,work]
def ts2nparray(d):
    tmin=d.t0
    dt=d.dt
    m=d.npts
    # There is likely a faster way to do this, but for now we want to 
    # be sure this looks like a 2d array as input to the plot function
    work=numpy.zeros(shape=[m,1])
    for j in range(m):
        work[j,0]=d.s[j]
    return [tmin,dt,work]
    
def wtvaplot(d,ranges=None,scale=1.0,fill_color='k',normalize=False, 
             cmap=None,title=None):
    """
    Wiggle trace variable area plotter for mspass ensemble objects. 
    """
    # We have to handle 3C ensembles specially to make 3 separate 
    # windows.   this logic is potentially confusing.  the else 
    # block handles all but 3C ensembles - common read structure 
    # makes a single plot call work for all 3 cases
    if(isinstance(d,mspass.SeismogramEnsemble)):
        # We always plot 3C data as 3 windows.  We extract each 
        # component and then call this function with a trivial 
        # recursion - only call itself once and only once
        title3c=title
        for i in range(3):
            dcomp=mspass.EnsembleComponent(d,i)
            if(title!=None):
                title3c='%s:%d' % (title,i)
            try:
                [t0,dt,section]=tse2nparray(dcomp)
                wtva_raw(section,t0,dt,ranges,scale,fill_color,normalize)
                if(cmap!=None):
                    image_raw(section,t0,dt,ranges,cmap)
                if(title3c!=None):
                    pyplot.title(title3c)
                pyplot.show()
            except RuntimeError as err:
                print(err)
                return None
    else:      
        try:
            # need to force these into the scope of this block
            plotdata=[]
            if(isinstance(d,mspass.TimeSeriesEnsemble)):
                plotdata=tse2nparray(d)
            elif(isinstance(d,mspass.Seismogram)):
                plotdata=seis2nparray(d)
            elif(isinstance(d,mspass.TimeSeries)):
                plotdata=ts2nparray(d)
            else:
                raise RuntimeError("wtvaplot - data received is not one supported by mspass")
            t0=plotdata[0]
            dt=plotdata[1]
            section=plotdata[2]
            wtva_raw(section,t0,dt,ranges,scale,fill_color,normalize)
            if(cmap!=None):
                image_raw(section,t0,dt,ranges,cmap)
            if(title!=None):
                pyplot.title(title)
            pyplot.show()
        except RuntimeError as err:
                print(err)
                return None
    return pyplot.gcf()
def imageplot(d,ranges=None,cmap=pyplot.cm.gray,aspect=None,vmin=None,vmax=None,
              title=None):
    """
    Image plotter for mspass ensemble objects. 
    """
    # We have to handle 3C ensembles specially to make 3 separate 
    # windows.   this logic is potentially confusing.  the else 
    # block handles all but 3C ensembles - common read structure 
    # makes a single plot call work for all 3 cases
    if(isinstance(d,mspass.SeismogramEnsemble)):
        # We always plot 3C data as 3 windows.  We extract each 
        # component and then call this function with a trivial 
        # recursion - only call itself once and only once
        title3c=title
        for i in range(3):
            dcomp=mspass.EnsembleComponent(d,i)
            if(title!=None):
                title3c='%s:%d' % (title,i)
            try:
                [t0,dt,section]=tse2nparray(dcomp)
                image_raw(section,t0,dt,ranges,cmap,aspect,vmin,vmax)
                if(title3c!=None):
                    pyplot.title(title3c)
                pyplot.show()
            except RuntimeError as err:
                print(err)
                return None
    else:      
        try:
            # need to force these into the scope of this block
            plotdata=[]
            if(isinstance(d,mspass.TimeSeriesEnsemble)):
                plotdata=tse2nparray(d)
            elif(isinstance(d,mspass.Seismogram)):
                plotdata=seis2nparray(d)
            elif(isinstance(d,mspass.TimeSeries)):
                plotdata=ts2nparray(d)
            else:
                raise RuntimeError("wtvaplot - data received is not one supported by mspass")
            t0=plotdata[0]
            dt=plotdata[1]
            section=plotdata[2]
            image_raw(section,t0,dt,ranges,cmap,aspect,vmin,vmax)
            if(title!=None):
                pyplot.title(title)
            pyplot.show()
        except RuntimeError as err:
                print(err)
                return None
    return pyplot.gcf()
      
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
    def __init__(self,scale=1.0,normalize=False,title=None):
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
        self.scale=scale
        self.title=title
        self.normalize=normalize
        # these have some interdependencies and are best altered only 
        # through the change_style method.  This may be unnecessary
        # as it may duplicate the call to change_style at the end, BUT
        # if the default changes these initial values do not need to be
        # changed
        self._style='colored_wtva'
        self._use_variable_area=True
        self._fill_color='k'  #black in matplotlib
        self._color_background=True
        self._color_map='seismic'
        # these are options to raw codes adapted from  fatiando a terra 
        # that are currently ignored.   Convert to args for __init__ if 
        # it proves useful to have them in the api
        self._ranges=None
        self._aspect=None
        self._vmin=None
        self._vmax=None
        # use change_style to simply default style
        self.change_style('colored_wtva')
    def change_style(self,newstyle,fill_color='k',color_map='seismic'):
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
          image - data are displayed as an image with the color_map parameter 
            defining the matplotlib color map used to map amplitudes into 
            a given color.   Note as with wtva the data are presumed to be 
            scaled to be of order 1.  color_map must not be None, which it 
            can be if the plot was changed from a previous style like wtva,
            or a RuntimeError exception will be thrown.
          colored_wtva is like wtva but the wtva plot is drawn on top of an 
            image plot.  Since this is the union of wtva and image a RuntimeError
            exception will be throw if either color_map or fill_color are null.
          wiggletrace will produce a standard line graph of each input as a 
            black line (that feature is currently frozen).   color_map and 
            fill_color are ignored for this style so no exceptions should 
            occur when the method is called with this value of newstyle. 
        """
        if(newstyle=='wtva'):
            if(fill_color==None):
                raise RuntimeError("SectionPlotter.change_style: wtva style requires fill_color to be a valid color code - received a None")
            self._style='wtva'
            self._color_background=False
            # force this when set this way
            self._color_map=None
            self._fill_color=fill_color
            self._use_variable_area=True
        elif(newstyle=='colored_wtva'):
            if(color_map==None):
                raise RuntimeError("SectionPlotter.change_style: colored_wtva style requires a color_map definition - received a None")
            if(fill_color==None):
                raise RuntimeError("SectionPlotter.change_style: colored_wtva style requires a fill_color definition - received a None")
            self._style='colored_wtva'
            self._color_background=True
            self._fill_color=fill_color
            self._color_map=color_map
            self._use_variable_area=True
        elif(newstyle=='image'):
            if(color_map==None):
                raise RuntimeError("SectionPlotter.change_style: image style requires a color_map definition - received a None")
            self._style='image'
            self._color_background=True
            self._fill_color=None
            self._use_variable_area=False
            self._color_map=color_map
        elif(newstyle=='wiggletrace'):
            self._style='wiggletrace'
            self._color_background=False
            self._color_map=None
            self._fill_color=None
            self._use_variable_area=False
        else:
            raise RuntimeError('SectionPlotter.change_style:  unknown style type='+newstyle)
    def plot(self,d):
        """
        Call this method to plot any data using the current style setup and any 
        details defined by public attributes.  
        
        :param d:  is the data to be plotted.   It can be any of the following:
            TimeSeries, Seismogram, TimeSeriesEnsemble, or SeismogramEnsemble.  
            If d is any other type the method will throw a RuntimeError exception.
        
        :Returns: a matplotlib.pylot plot handle.
        :rtype: The plot handle is what matplotlib.pyplot calls a gcf.  It 
          is the return of pyplot.gcf() and can be used to alter some properties
          of the figure.  See matplotlib documentation.
        """
        # these are all handled by the same function with argument combinations defined by 
        # change_style determining the behavior. 
        if(self._style=='wtva' or self._style=='colored_wtva' or self._style=='wiggletrace'):
            handle=wtvaplot(d,self._ranges,self.scale,self._fill_color,self.normalize,self._color_map,self.title)
            return handle
        elif(self._style=='image'):
            handle=imageplot(d,self._ranges,self._color_map,self._aspect,self._vmin,self._vmax,self.title)
            return handle
        else:
            raise RuntimeError('SectionPlotter.plot:  internal style definition='+self._style+' which is illegal.  Run change_style method')

            
    

