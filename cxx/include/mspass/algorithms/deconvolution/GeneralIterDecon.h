#ifndef __SIIMPLE_GENERAL_ITER_DECON__
#define __SIIMPLE_GENERAL_ITER_DECON__
#include <vector>
#include "mspass/algorithms/deconvolution/ComplexArray.h"
#include "mspass/utility/Metadata.h"
#include "mspass/algorithms/deconvolution/ScalarDecon.h"
#include "mspass/algorithms/deconvolution/FFTDeconOperator.h"
#include "mspass/utility/dmatrix.h"
#include "mspass/utility/AntelopePf.h"
#include "mspass/seismic/TimeWindow.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/seismic/Seismogram.h"
namespace mspass::algorithms::deconvolution{
/* Wang's original version allowed XCORR here - dropped for now as this
code assumes the decon produces a zero phase ideal output while in every
case the original XCORR iterative method uses the raw wavelet and cross
correlation. */

enum IterDeconType {WATER_LEVEL,LEAST_SQ,MULTI_TAPER};
class ThreeCSpike
{
public:
    /*! The column position where this spike should be placed in parent
    three component dmatrix where time is the column field */
    int col;
    /*! This holds the amplitude of this spike as three components */
    double u[3];
    /*! L2 norm of u savd for efficiency to avoid repeated calculations */
    double amp;
    /* This is the primary constructor for oneo of thise.  In the iterative
    sequnce of generalized iterative decon we find the column with maximum
    amplitude and point this constructor at the right column.  The
    parent dmatrix is then updated subtracting the inverse wavelet at
    this lag. */
    ThreeCSpike(mspass::utility::dmatrix& d, int k);
    /*! Copy consructor.  Could be defaulted, I think, but best to
    create it explicitly. */
    ThreeCSpike(const ThreeCSpike& parent);
    /*! Makign sure operator= is also essential be sure STL containers
    work correctly */
    ThreeCSpike& operator=(const ThreeCSpike& parent);
};
/*! \brief Implements the Generalized Iterative Method of Wang and Pavlis.

This class is an extension of the idea o the generalized iterative method
described in the paper by Wang and Pavlis.   Their original paper worked
on scalar seismograms.  This implemenation differs by iterating on the
vector data effectively reducing a 3c seismogram to a sequence of
3D vectors at computed lags.   The iterative reduction then iterates
on the data matrix.   In addition, Wang's original implementation did the
iteration by repeated forward and inverse fourier transforms.  This algorithm
works completely in the time domain.

The class retains a concept Wang used in the original implementation.
Many OOP books stress the idea of construction is initialization.   That is
not pruddent fo this case since construction involves some work that we
do not want to repeat for each new seismogram.   Hence, construction only
initializes parameters required to run the algorithm. Calculation is
triggered by calling one of the two overloaded load methods.  The user
should be warned, however, that we did not build ina  test to assure
the noise and signal windows are consistent.  Anyone wanting to use this
code outside of applications developed by the authors must recognize That
the assure consistency they need to call loadnoise before load OR call
only the load method that includes a signal and noise window.
*/

class GeneralIterDecon: public ScalarDecon
{
public:
    /*! \brief Create an initialize an operator for subseqquent processing.

    This is the primary constructor for this object.   The PfStyleMetadata
    object is expected to contain a suite of parameters used to define
    how this operator will behave along with what inverse filter is to
    be constructed ans applied before the iterative algorithm is applied.
    The pf is required to have multiple Arr sections the components.  */
    GeneralIterDecon(mspass::utility::AntelopePf &md);
    GeneralIterDecon(const GeneralIterDecon &parent);
    void changeparameter(const mspass::utility::Metadata &md) {
        this->preprocessor->changeparameter(md);
    };
    int load(const mspass::seismic::CoreSeismogram& d, mspass::seismic::TimeWindow dwin);
    int loadnoise(const mspass::seismic::CoreSeismogram& d, mspass::seismic::TimeWindow nwin);
    /*! \brief Load all needed data and process.

    This method is little more than a call to loadnoise followed
    immediately by a call to load that is assumed to initiate the
    computation. */
    int load(const mspass::seismic::CoreSeismogram& d, mspass::seismic::TimeWindow dwin, mspass::seismic::TimeWindow nwin);
    void process();
    ~GeneralIterDecon();
    mspass::seismic::CoreSeismogram getresult();
    mspass::seismic::CoreTimeSeries ideal_output() {
        return this->preprocessor->ideal_output();
    };
    mspass::seismic::CoreTimeSeries actual_output() {
        return this->preprocessor->actual_output();
    };
    mspass::seismic::CoreTimeSeries inverse_wavelet() {
        return this->preprocessor->inverse_wavelet();
    };
    mspass::seismic::CoreTimeSeries inverse_wavelet(double t0parent) {
        return this->preprocessor->inverse_wavelet(t0parent);
    };
    mspass::utility::Metadata QCMetrics();
private:
    /* These are data at different stages of process.  d_all is the
    largest signal window that is assumed to have been initialized by the
    load method for this object.  d_decon is the
    result of apply the preprocessor (signal processing) deconvolution to
    all three components. d_decon is computed from d, which is a windowed
    version of the input data received by the load method.  It have a
    time duration less than or at least equal to that of d_all.
    r is the residual, which is accumulated during the iterative method.
    The time duraction of r is the same as d.   It is initalized by
    convolving the inverse filter with d_all.
    n is the noise data.  It should normally be at least as long a d_all*/
    mspass::seismic::CoreSeismogram d_all,d_decon,r,n;
    /* We save the set of data lengths for clarity and a minor bit efficiency.
    ndwin is the number of samples in d_all and r.
    nnwin is the number of samples in n.
    nfft is the size of d_decon */
    int ndwin, nnwin,nfft;
    /* this is the time shift in sample applied by preprocessor*/
    int time_shift;
    /* Save the TimeWindow objects hat define the extent of d_all, d_decon,
    and n.   Some things need at least some of these downstream */
    mspass::seismic::TimeWindow dwin,nwin,fftwin;
    /*! For preprocessor algorithms that are scalare we specify which channel
    is used to define the noise for regularization */
    int noise_component;
    /*! The algorithm accumulates spikes in this linked list.   To create
    the result as a normal time series this is converted to a conventional
    form in the getresult method */
    std::list<ThreeCSpike> spikes;
    /*! Penalty function weights.

    This algorithm uses a penalty function that downweights a range of lags
    around the time of each spike subtracted in the iteration.   This stl
    vector contains the accumulated weighting function at the end of the
    iteration.  It is used for QC */
    std::vector<double> lag_weights;
    /* This vector contains the function time shifted and added to lag_weights
    vector after each iteration.   */
    std::vector<double> wtf;
    int nwtf;  //size of wtf - cached because wtf is inside the deepest loop

    /* This is a pointer to the BasicDeconOperator class used for preprocessing
    Classic use of inheritance to simplify the api. */
    ScalarDecon *preprocessor;

    /* This defines a shaping wavelet applied to output.  The inverse filter
    preprocessing algorithm is assumed to have it's own embedded shaping wavelet.
    i.e. this is the wavelet convolved with the output spike sequence. */
    ShapingWavelet shapingwavelet;
    /* This parameter is set in the constructor.  It would normally be half the length
    of the fir representation of the inverse wavelet. (winv_fir below)*/
    int wavelet_pad;
    /*! \brief Shorted inverse wavelet used for iteration.

    The iteration is done here in the time domain.  The wavelet returned by
    the preprocessor algorithm will commonly have lots of zeros or small numbers
    outside a central area.  The constructor to needs to define how that
    long wavelet is shortened to build this one. winv_fir is the inverse
    wavelet vector and actual_o_fir is the ideal output fir filter vector.
    */
    std::vector<double> winv_fir, actual_o_fir;
    int actual_o_0;  //offset from sample zero for zero lag position
    IterDeconType decon_type;
    /* This is called by the constructor to create the wtf penalty function */
    void construct_weight_penalty_function(const mspass::utility::Metadata& md);
    /*! Subtract current spike signal from data.

    \param spk - vector amplitude and lag of spike - subtract ideal
    output at this lag.
    */
    void update_residual_matrix(ThreeCSpike spk);
    /*! Updates the lag weight vector.

    This implementation has an enhanced time weighting function
    beyond that given in Wang and Pavlis's paper.   This private method
    updates the weight vector using the lag position (in samples) of the
    current spike. */
    void update_lag_weights(int col);
    /*! This private method is called after load noise to se the quantity
    resid_linf_floor = convergence criteria on amplitude.  That paramters is
    computed from sorting the filtered, preevent noise and setting the
    threshold from a probability level.  Returns the computed floor but
    also sets resid_linf_floor in this base object */
    double compute_resid_linf_floor();
    /*! Apply all convergence tests. Returns false to terminate the iteration
    loop.  This puts all such calculations in one place. */
    bool has_not_converged();
    /* These are convergence attributes.   lw_inf indicates Linf norm of
    lag_weight array, lw_l2 is L2 metric of lag_weight, resid_inf is Linf
    norm of residual vector, and resid_l2 is L2 of resid matrix.   prev
    modifier means the size of that quantity in the previous iteration.  initial
    means initial value at the top of the loop.*/
    double lw_linf_initial, lw_linf_prev;
    double lw_l2_initial, lw_l2_prev;
    double resid_linf_initial, resid_linf_prev;
    double resid_l2_initial, resid_l2_prev;
    /* These are convergence paramwters for the different tests */
    int iter_count, iter_max;  // actual iteration count and ceiling to break loop
    /*lw metrics are scaled with range of 0 to 1.  l2 gets scaled by number of
    points and so can use a similar absolute scale. */
    double lw_linf_floor, lw_l2_floor;
    /* We use a probability level to define the floor Linf of the residual
    matrix.   For L2 we use the conventional fractional improvment metric. */
    double resid_linf_prob, resid_linf_floor;
    double resid_l2_tol;


    /* DEBUG attributes and methods.  These should be deleted after testing. */
    std::vector<double> lw_linf_history,lw_l2_history,resid_l2_history,resid_linf_history;
    void print_convergence_history(std::ostream& ofs)
    {
        ofs << "lw_inf lw_l2 resid_l2, resid_linf"<<std::endl;
        for(int i=0; i<iter_count; ++i)
        {
            if(i<lw_linf_history.size())
            {
                ofs<<lw_linf_history[i]<<" ";
            }
            else
            {
                ofs<<"xxxx " ;
            }
            if(i<lw_l2_history.size())
            {
                ofs<<lw_l2_history[i]<<" ";
            }
            else
            {
                ofs<<"xxxx " ;
            }
            if(i<resid_linf_history.size())
            {
                ofs<<resid_linf_history[i]<<" ";
            }
            else
            {
                ofs<<"xxxx " ;
            }
            if(i<resid_l2_history.size())
            {
                ofs<<resid_l2_history[i]<<" ";
            }
            else
            {
                ofs<<"xxxx " ;
            }
            ofs<<std::endl;
        }
    }
};
}
#endif
