#include <math.h>
#include "mspass/utility/MsPASSError.h"
#include "mspass/seismic/CoreSeismogram.h"
namespace mspass {
using namespace mspass;
/* This file contains helper procedures for CoreSeismogram objects.  Most
are truly procedural and take a CoreSeismogram object or a Ensemble
object and return one or the other.

This file was modified from code developed earlier in antelope contrib.   Most
changes are little more than name changes.

Author:  Gary L. Pavlis
	Indiana University
	pavlis@indiana.edu
*/


/* This function converts a CoreSeismogram from an absolute time standard to
an arrival time reference frame using an arrival time stored in the
metadata area of the object and referenced by a variable keyword.
A time window is relative time units defines the portion to be extracted.
If the window is larger than the data the whole array of data is copied
to the result.  If the window is smaller a subset is returned and
appropriate parameters are updated.

Arguments:
	tcsi - input data.  Assumed to be in the reference frame that
		matches the arrival time to be used to define time window
	arrival_time_key - metadata keyword string used to extract the
		time used to define time 0.  i.e. this is the keyword
		used to access a metadata field that defines the time
		that will become time 0 o the result.
	tw - TimeWindow in relative time units wrt arrival time defining
		section of data to be extracted.  Time 0 of the result
		will be the arrival time.

*/

shared_ptr<CoreSeismogram> ArrivalTimeReference(CoreSeismogram& tcsi,
        string arrival_time_key,
        TimeWindow tw)
{
    double atime;
    string base_error_message("ArrivalTimeReference: ");
    try
    {
        atime = tcsi.get_double(arrival_time_key);
        // Intentionally use the base class since the contents are discarded
        // get_double currently would throw a MetadataGetError
    } catch (MetadataGetError& mde)
    {
        throw MsPASSError(base_error_message
                          + arrival_time_key
                          + string(" not found in CoreSeismogram object"),
                          ErrorSeverity::Invalid);
    }
    // We have to check this condition because ator will do nothing if
    // time is already relative and this condition cannot be tolerated
    // here as we have no idea what the time standard might be otherwise
    if(tcsi.tref == TimeReferenceType::Relative)
        throw MsPASSError(string("ArrivalTimeReference:  ")
                          + string("received data in relative time units\n")
                          + string("Cannot proceed as timing is ambiguous"),
                          ErrorSeverity::Invalid);

    // start with a clone of the original
    shared_ptr<CoreSeismogram> tcso(new CoreSeismogram(tcsi));
    tcso->ator(atime);  // shifts to arrival time relative time reference
    // Simply return a copy of traces marked dead
    if(!(tcso->live)) return(tcso);

    // Extracting a subset of the data is not needed when the requested
    // time window encloses all the data
    // Note an alternative approach is to pad with zeros and mark ends as
    // a gap, but here I view ends as better treated with variable
    // start and end times
    if( (tw.start > tcso->t0)  || (tw.end<tcso->time(tcso->ns-1) ) )
    {
        int jstart, jend;
        int ns_to_copy;
        int i,j,jj;
        jstart = tcso->sample_number(tw.start);
        jend = tcso->sample_number(tw.end);
        if(jstart<0) jstart=0;
        if(jend>=tcso->ns) jend = tcso->ns - 1;
        ns_to_copy = jend - jstart + 1;
        // This is a null trace so mark it dead in this condition
        if(ns_to_copy<0)
        {
            ns_to_copy=0;
            tcso->live=false;
            tcso->u=dmatrix(1,1);
            tcso->ns=0;
        }
        else
        {
            // This is not the fastest way to do this, but it is
            // clearer and the performance hit should not be serious
            // old advice:  make it work before you make it fast
            tcso->u = dmatrix(3,ns_to_copy);
            tcso->ns=ns_to_copy;
            for(i=0; i<3; ++i)
                for(j=0,jj=jstart; j<ns_to_copy; ++j,++jj)
                    tcso->u(i,j)=tcsi.u(i,jj);
            tcso->t0 += (tcso->dt)*static_cast<double>(jstart);
            //
            // This is necessary to allow for a rtoa (relative
            // to absolute time) conversion later.
            //
            if(jstart>0)
            {
                double stime=atime+tcso->t0;
                tcso->put("time",stime);
                // this one may not really be necessary
                tcso->put("endtime",atime+tcso->endtime());
            }
        }
    }
    return(tcso);
}
/* Similar function to above but processes an entire ensemble.  The only
special thing it does is handle exceptions.  When the single object
processing function throws an exception the error is printed and the
object is simply not copied to the output ensemble */
shared_ptr<Ensemble<CoreSeismogram>> ArrivalTimeReference(Ensemble<CoreSeismogram>& tcei,
        string arrival_time_key,
        TimeWindow tw)
{
    int nmembers=tcei.member.size();
    // use the special constructor to only clone the metadata and
    // set aside slots for the new ensemble.
    shared_ptr<Ensemble<CoreSeismogram>>
    tceo(new Ensemble<CoreSeismogram>(dynamic_cast<Metadata&>(tcei),
                                    nmembers));
    tceo->member.reserve(nmembers);  // reserve this many slots for efficiency
    // We have to use a loop instead of for_each as I don't see how
    // else to handle errors cleanly here.
    vector<CoreSeismogram>::iterator indata;
    for(indata=tcei.member.begin(); indata!=tcei.member.end(); ++indata)
    {
        try {
            shared_ptr<CoreSeismogram>
            tcs(ArrivalTimeReference(*indata,arrival_time_key,tw));
            tceo->member.push_back(*tcs);
        } catch ( MsPASSError& serr)
        {
            serr.log_error();
            cerr << "This CoreSeismogram was not copied to output ensemble"<<endl;
        }
    }
    return(tceo);
}

/* This is a procedural form of one of the rotate method and is a bit redundant. */
void HorizontalRotation(CoreSeismogram& d, double phi)
{
    double tmatrix[3][3];
    double a,b;
    a=cos(phi);
    b=sin(phi);
    tmatrix[0][0]=a;
    tmatrix[1][0]=-b;
    tmatrix[2][0]=0.0;
    tmatrix[0][1]=b;
    tmatrix[1][1]=a;
    tmatrix[2][1]=0.0;
    tmatrix[0][2]=0.0;
    tmatrix[1][2]=0.0;
    tmatrix[2][2]=1.0;
    d.transform(tmatrix);
}
/* Extracts one component from a 3c seismogram returning the
 result as a TimeSeries object.  Any exceptions are
simply rethrown.

*/
CoreTimeSeries ExtractComponent(const CoreSeismogram& tcs,const int component)
{
    try {
        CoreTimeSeries ts(dynamic_cast<const BasicTimeSeries&>(tcs),
                                  dynamic_cast<const Metadata&>(tcs));
        /* Important - need to clear vector or we get nothing */
        ts.s.clear();
        double *ptr;
        dmatrix *uptr;
        if(tcs.live)
            for(int i=0; i<tcs.ns; ++i)
            {
                uptr=const_cast<dmatrix *>(&(tcs.u));
                ptr=uptr->get_address(component,i);
                ts.s.push_back(*ptr);
            }
        return(ts);
    }
    catch (...)
    {
        throw;
    }
}

} // end mspass namespace encapsulation
