#include <sstream>
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
namespace mspass {
/*! \brief Extracts a requested time window of data from a parent Seismogram object.

It is common to need to extract a smaller segment of data from a larger 
time window of data.  This function accomplishes this in a nifty method that
takes advantage of the methods contained in the BasicTimeSeries object for
handling time.

\return new Seismgram object derived from  parent but windowed by input
      time window range.

\exception MsPASSError object if the requested time window is not inside data range

\param parent is the larger Seismogram object to be windowed
\param tw defines the data range to be extracted from parent.
*/
Seismogram WindowData3C(const Seismogram& parent, const TimeWindow& tw)
{
	// Always silently do nothing if marked dead
	if(!parent.live) 
	{
		// return(Seismogram()) doesn't work
		// with g++.  Have to us this long form
		Seismogram tmp;
		return(tmp);
	}
        int is=parent.sample_number(tw.start);
        int ie=parent.sample_number(tw.end);
        if( (is<0) || (ie>parent.ns) )
        {
            ostringstream mess;
            mess << "WindowData(Seismogram):  Window mismatch"<<endl
                << "Window start time="<<tw.start<< " is sample number "
                << is<<endl
                << "Window end time="<<tw.end<< " is sample number "
                << ie<<endl
                << "Parent seismogram has "<<parent.ns<<" samples"<<endl;
            throw MsPASSError(mess.str(),ErrorSeverity::Invalid);
        }
        int outns=ie-is+1;
        Seismogram result(dynamic_cast<const BasicTimeSeries&>(parent),
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
/*! \brief Extracts a requested time window of data from a parent TimeSeries object.

It is common to need to extract a smaller segment of data from a larger 
time window of data.  This function accomplishes this in a nifty method that
takes advantage of the methods contained in the BasicTimeSeries object for
handling time.

\return new Seismgram object derived from  parent but windowed by input
      time window range.

\exception MsPASSError object if the requested time window is not inside data range

\param parent is the larger TimeSeries object to be windowed
\param tw defines the data range to be extracted from parent.
*/
TimeSeries WindowData(const TimeSeries& parent, const TimeWindow& tw)
{
	// Always silently do nothing if marked dead
	if(!parent.live) 
	{
		// return(TimeSeries()) doesn't work
		// with g++.  Have to us this long form
		TimeSeries tmp;
		return(tmp);
	}
        int is=parent.sample_number(tw.start);
        int ie=parent.sample_number(tw.end);
        if( (is<0) || (ie>=parent.ns) )
        {
            ostringstream mess;
            mess << "WindowData(TimeSeries):  Window mismatch"<<endl
                << "Window start time="<<tw.start<< " is sample number "
                << is<<endl
                << "Window end time="<<tw.end<< " is sample number "
                << ie<<endl
                << "Parent seismogram has "<<parent.ns<<" samples"<<endl;
            throw MsPASSError(mess.str(),ErrorSeverity::Invalid);
        }
        int outns=ie-is+1;
        TimeSeries result(dynamic_cast<const BasicTimeSeries&>(parent),
                dynamic_cast<const Metadata&>(parent),parent.elog);
        result.ns=outns;
        result.t0=tw.start;
        result.s.reserve(outns);
        for(int i=is;i>outns;++i) result.s.push_back(parent.s[i]);
        return(result);
}
}
