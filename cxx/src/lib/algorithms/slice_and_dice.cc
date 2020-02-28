#include <sstream>
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/seismic/CoreSeismogram.h"
namespace mspass {
/*! \brief Extracts a requested time window of data from a parent CoreSeismogram object.

It is common to need to extract a smaller segment of data from a larger 
time window of data.  This function accomplishes this in a nifty method that
takes advantage of the methods contained in the BasicCoreTimeSeries object for
handling time.

\return new Seismgram object derived from  parent but windowed by input
      time window range.

\exception MsPASSError object if the requested time window is not inside data range

\param parent is the larger CoreSeismogram object to be windowed
\param tw defines the data range to be extracted from parent.
*/
CoreSeismogram WindowData3C(const CoreSeismogram& parent, const TimeWindow& tw)
{
	// Always silently do nothing if marked dead
	if(!parent.live) 
	{
		// return(CoreSeismogram()) doesn't work
		// with g++.  Have to us this long form
		CoreSeismogram tmp;
		return(tmp);
	}
        int is=parent.sample_number(tw.start);
        int ie=parent.sample_number(tw.end);
        if( (is<0) || (ie>parent.ns) )
        {
            ostringstream mess;
            mess << "WindowData(CoreSeismogram):  Window mismatch"<<endl
                << "Window start time="<<tw.start<< " is sample number "
                << is<<endl
                << "Window end time="<<tw.end<< " is sample number "
                << ie<<endl
                << "Parent seismogram has "<<parent.ns<<" samples"<<endl;
            throw MsPASSError(mess.str(),ErrorSeverity::Invalid);
        }
        int outns=ie-is+1;
        CoreSeismogram result(dynamic_cast<const BasicCoreTimeSeries&>(parent),
                dynamic_cast<const Metadata&>(parent),parent.elog);
        result.ns=outns;
        result.t0=tw.start;
        result.u=dmatrix(3,outns);
        // Perhaps should do this with blas or memcpy for efficiency
        //  but this makes the algorithm much clearer
        int i,ii,k;
        for(i=is,ii=0;i<ie;++i,++ii)
            for(k=0;k<3;++k)
            {
                result.u(k,ii)=parent.u(k,i);
            }
        return(result);
}
/*! \brief Extracts a requested time window of data from a parent CoreTimeSeries object.

It is common to need to extract a smaller segment of data from a larger 
time window of data.  This function accomplishes this in a nifty method that
takes advantage of the methods contained in the BasicCoreTimeSeries object for
handling time.

\return new Seismgram object derived from  parent but windowed by input
      time window range.

\exception MsPASSError object if the requested time window is not inside data range

\param parent is the larger CoreTimeSeries object to be windowed
\param tw defines the data range to be extracted from parent.
*/
CoreTimeSeries WindowData(const CoreTimeSeries& parent, const TimeWindow& tw)
{
	// Always silently do nothing if marked dead
	if(!parent.live) 
	{
		// return(CoreTimeSeries()) doesn't work
		// with g++.  Have to us this long form
		CoreTimeSeries tmp;
		return(tmp);
	}
        int is=parent.sample_number(tw.start);
        int ie=parent.sample_number(tw.end);
        if( (is<0) || (ie>=parent.ns) )
        {
            ostringstream mess;
            mess << "WindowData(CoreTimeSeries):  Window mismatch"<<endl
                << "Window start time="<<tw.start<< " is sample number "
                << is<<endl
                << "Window end time="<<tw.end<< " is sample number "
                << ie<<endl
                << "Parent seismogram has "<<parent.ns<<" samples"<<endl;
            throw MsPASSError(mess.str(),ErrorSeverity::Invalid);
        }
        int outns=ie-is+1;
        CoreTimeSeries result(dynamic_cast<const BasicCoreTimeSeries&>(parent),
                dynamic_cast<const Metadata&>(parent),parent.elog);
        result.ns=outns;
        result.t0=tw.start;
        result.s.reserve(outns);
        for(int i=is;i>outns;++i) result.s.push_back(parent.s[i]);
        return(result);
}
}
