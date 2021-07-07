#include <sstream>
#include "mspass/utility/MsPASSError.h"
#include "mspass/algorithms/TimeWindow.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
namespace mspass::algorithms
{
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;

/*! \brief Extracts a requested time window of data from a parent coreSeismogram object.

It is common to need to extract a smaller segment of data from a larger
time window of data.  This function accomplishes this in a nifty method that
takes advantage of the methods contained in the BasicTimeSeries object for
handling time.

\return new CoreSeismogram object derived from  parent but windowed by input
      time window range.

\exception MsPASSError object if the requested time window is not inside data range

\param parent is the larger CoreSeismogram object to be windowed
\param tw defines the data range to be extracted from parent.
*/
CoreSeismogram WindowData(const CoreSeismogram& parent, const TimeWindow& tw)
{
	// Always silently do nothing if marked dead
	if(parent.dead())
	{
		// return(CoreSeismogram()) doesn't work
		// with g++.  Have to us this long form
		CoreSeismogram tmp;
		return(tmp);
	}
  int is=parent.sample_number(tw.start);
  int ie=parent.sample_number(tw.end);
  if( (is<0) || (ie>parent.npts()) )
  {
      ostringstream mess;
      mess << "WindowData(CoreSeismogram):  Window mismatch"<<endl
                << "Window start time="<<tw.start<< " is sample number "
                << is<<endl
                << "Window end time="<<tw.end<< " is sample number "
                << ie<<endl
                << "Parent has "<<parent.npts()<<" samples"<<endl;
      throw MsPASSError(mess.str(),ErrorSeverity::Invalid);
  }
  int outns=ie-is+1;
	CoreSeismogram result(parent);
  result.u=dmatrix(3,outns);
	result.set_npts(outns);
	result.set_t0(tw.start);
  // Perhaps should do this with blas or memcpy for efficiency
  //  but this makes the algorithm much clearer
  int i,ii,k;
  for(i=is,ii=0;i<=ie;++i,++ii)
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
	if(parent.dead())
	{
		// return(CoreTimeSeries()) doesn't work
		// with g++.  Have to us this long form
		CoreTimeSeries tmp;
		return(tmp);
	}
  int is=parent.sample_number(tw.start);
  int ie=parent.sample_number(tw.end);
	//Ridiculous (int) case to silence a bogus compiler warning
  if( (is<0) || (ie>=((int)parent.npts())) )
  {
      ostringstream mess;
          mess << "WindowData(CoreTimeSeries):  Window mismatch"<<endl
              << "Window start time="<<tw.start<< " is sample number "
              << is<<endl
              << "Window end time="<<tw.end<< " is sample number "
              << ie<<endl
              << "Parent has "<<parent.npts()<<" samples"<<endl;
      throw MsPASSError(mess.str(),ErrorSeverity::Invalid);
  }
  int outns=ie-is+1;
	CoreTimeSeries result(parent);
	result.s.reserve(outns);
	result.set_npts(outns);
	result.set_t0(tw.start);
	// Necessary to use the push_back method below or we get leading zeros
	result.s.clear();

  for(int i=is;i<=ie;++i) result.s.push_back(parent.s[i]);
  return(result);
}

/*! \brief Extracts a requested time window of data from a parent Seismogram object.

It is common to need to extract a smaller segment of data from a larger
time window of data.  This function accomplishes this in a nifty method that
takes advantage of the methods contained in the BasicTimeSeries object for
handling time.

This function differs from the CoreSeismogram version in error handling.
It follows the MsPASS model of posting errors to elog and returning a
dead object for unrecoverable errors.

\return new Seismogram object derived from  parent but windowed by input
      time window range.

\exception MsPASSError object if the requested time window is not inside data range

\param parent is the larger Seismogram object to be windowed
\param tw defines the data range to be extracted from parent.
*/
Seismogram WindowData(const Seismogram& parent, const TimeWindow& tw)
{
	// Always silently do nothing if marked dead
	if(parent.dead())
	{
		return parent;
	}
  int is=parent.sample_number(tw.start);
  int ie=parent.sample_number(tw.end);
  if( (is<0) || (ie>parent.npts()) )
  {
      ostringstream mess;
      mess << "WindowData(Seismogram):  Window mismatch"<<endl
                << "Window start time="<<tw.start<< " is sample number "
                << is<<endl
                << "Window end time="<<tw.end<< " is sample number "
                << ie<<endl
                << "Parent has "<<parent.npts()<<" samples"<<endl;
			/* Doing this full copy is a bit inefficient, but it avoids needing
			to remove the const qualifier on parent.*/
			Seismogram dead_return(parent);
			dead_return.kill();
      dead_return.elog.log_error("WindowData",mess.str(),ErrorSeverity::Invalid);
			/* In this case it is appropriate to clear the data array  - this does that*/
			dead_return.set_npts(0);
			return dead_return;
  }
  int outns=ie-is+1;
	Seismogram result(parent);
  result.u=dmatrix(3,outns);
	result.set_npts(outns);
	result.set_t0(tw.start);
  // Perhaps should do this with blas or memcpy for efficiency
  //  but this makes the algorithm much clearer
  int i,ii,k;
  for(i=is,ii=0;i<=ie;++i,++ii)
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
	if(parent.dead())
	{
		// return(TimeSeries()) doesn't work
		// with g++.  Have to us this long form
		TimeSeries tmp;
		return(tmp);
	}
  int is=parent.sample_number(tw.start);
  int ie=parent.sample_number(tw.end);
	//Ridiculous (int) case to silence a bogus compiler warning
  if( (is<0) || (ie>=((int)parent.npts())) )
  {
      ostringstream mess;
          mess << "WindowData(TimeSeries):  Window mismatch"<<endl
              << "Window start time="<<tw.start<< " is sample number "
              << is<<endl
              << "Window end time="<<tw.end<< " is sample number "
              << ie<<endl
              << "Parent has "<<parent.npts()<<" samples"<<endl;
			/* Marking this copy is a bit inefficient, but necessary unless we
			wanted to drop the const qualifier onparent*/
			TimeSeries dret(parent);
			dret.kill();
			/* Here appropriate to clear the data array  - no reason to carry
			around baggages */
			dret.set_npts(0);
			dret.elog.log_error("WindowData",mess.str(),ErrorSeverity::Invalid);
			return dret;
  }
  int outns=ie-is+1;
	TimeSeries result(parent);
	result.s.reserve(outns);
	result.set_npts(outns);
	result.set_t0(tw.start);
	// Necessary to use the push_back method below or we get leading zeros
	result.s.clear();

  for(int i=is;i<=ie;++i) result.s.push_back(parent.s[i]);
  return(result);
}
} // end mspass namespace
