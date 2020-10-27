#ifndef __DECON_H__
#define __DECON_H__
namespace mspass::algorithms::deconvolution{
double *rickerwavelet(float fpeak, float dt, int n);
double *gaussian(float sigma, float dt, int n);
/*! \brief Creates frequency domain window as a 0th order slepian wavelet.

A novel concept this deconvolution package implements is the idea of using
a 0th order Slepian function to define an (equivalent) shaping wavelet in
the time domain.   Note this is the inverse form of Slepian functions to
that used in multitaper methods.  There the Slepians are used to define
time domain windows.   Here we use Slepians in the inverse direction.
This function should be called to window frequency domain data.  Since
windows in frequency = convolution in time there is an equivalent time
domain pulse that has minimum possible sidebands for the specified
time-bandwidth product.   Time-bandwidth product is a nondimensional
scaling concept.  It enters here in a way better left to a higher
level interface.   Here we control the bandwidth be defining the size
of the frequency domain window to be created (n) in samples.
In applications n must be less than or equal to the number of samples
in the frequency domain representation of the data.   That number
may be longer than the data duration because of padding.   However,
you are asking for problems if the n is greater than the parent
time series length.

\param tbb is the time bandwidth product to use to generate the window
  (Note increasing tbp will widen the equivalent time domain pulse.   Pulse
  duration will be approximately tbp*N/n samples where N is the total
  DFT length)
\param n is the length of the 0th order slepian function returned for
  given tbp.   See tbp description for how n interacts with tbp to
  define the shaped pulse duration.

\param double array of length n.  Array is expected to be used as a window
function in the frequency domain BUT will nearly always need to undergo a
circular shift to put the peak at 0.   NOT done here to avoid confusion.
*/
double *slepian0(double tbp,int n);
} 
#endif
