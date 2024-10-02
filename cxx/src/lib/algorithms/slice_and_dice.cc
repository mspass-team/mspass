#include <sstream>
#include "mspass/utility/MsPASSError.h"
#include "mspass/algorithms/TimeWindow.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/keywords.h"
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
  /* This calculation was used in earlier versions but was found to 
     differ by 1 depending on the subsample timing of t0 of parent.  the 
     problem is that sample_number uses rounding which can produce that
     effect due to a subtle interaction with tw.start and tw.end 
     relative to the sample grid. 
  int ie=parent.sample_number(tw.end);
  */
  int outns,ie;
  outns = round((tw.end-tw.start)/parent.dt()) + 1;  
  ie = is + outns - 1;
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
	CoreSeismogram result(parent);
  result.u=dmatrix(3,outns);
	result.set_npts(outns);
  /* Using the time method here preserves subsample timing.*/
	result.set_t0(parent.time(is));
  // Perhaps should do this with blas or memcpy for efficiency
  //  but this makes the algorithm much clearer
  int i,ii,k;
  for(i=is,ii=0;i<=ie && i<parent.npts() && ii<outns;++i,++ii)
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
  /* This calculation was used in earlier versions but was found to 
     differ by 1 depending on the subsample timing of t0 of parent.  the 
     problem is that sample_number uses rounding which can produce that
     effect due to a subtle interaction with tw.start and tw.end 
     relative to the sample grid. 
  int ie=parent.sample_number(tw.end);
  */
  int outns,ie;
  outns = round((tw.end-tw.start)/parent.dt()) + 1;  
  ie = is + outns - 1;
	//Ridiculous (int) case to silence a bogus compiler warning
  if( (is<0) || (ie>((int)parent.npts())) )
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
	CoreTimeSeries result(parent);
	result.s.reserve(outns);
	result.set_npts(outns);
  /* Using the time method here preserves subsample timing.*/
	result.set_t0(parent.time(is));
	// Necessary to use the push_back method below or we get leading zeros
	//result.s.clear();
  //for(int i=is;i<=ie && i<parent.npts();++i) result.s.push_back(parent.s[i]);
  int ii,i;
  for(ii=0, i=is; i<=ie && i<parent.npts() && ii<outns; ++i, ++ii)
    result.s[ii]=parent.s[i];
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
  /* This calculation was used in earlier versions but was found to 
     differ by 1 depending on the subsample timing of t0 of parent.  the 
     problem is that sample_number uses rounding which can produce that
     effect due to a subtle interaction with tw.start and tw.end 
     relative to the sample grid. 
  int ie=parent.sample_number(tw.end);
  */
  int outns,ie;
  outns = round((tw.end-tw.start)/parent.dt()) + 1;  
  ie = is + outns - 1;
  if( (is<0) || (ie>=parent.npts()) )
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
	/* MAINTENANCE ISSUE:  The original implementation of this code used a copy constructor
	here to initalize result.   We realized that was very inefficient when
	slicing down long input signals like data blocked in day files.
	We converted to this algorithm BUT be warned it is somewhat fragile
	as it depends on some side effects of the implementation of Seismogram
	that could break it if the following assumptions changes:
	1.  The constructor we use calls the set_npts method
	2.  that constructor also syncs npts metadata
	3.  The constructor initializes the dmatrix u to zeros with length
			determined from the set_npts result.
	Less important is we have to add the load_history call here or history
	would be lost.  Reason is the constructor uses CoreTimeSeries. */
	BasicTimeSeries btstmp(dynamic_cast<const BasicTimeSeries&>(parent));
	btstmp.set_npts(outns);
  /* Using the time method here preserves subsample timing.*/
	btstmp.set_t0(parent.time(is));
	/* WARNING MAINTENANCE ISSUE:  this is less than ideal fix for a problem
	found when debugging the revision of this algorithm to improve its
	performance May 2022.  the constuctor called here assumes the
	value of npts stored in Metadata is definitive creating an inconsistency
	that led to seg faults.   The following is a workaround that negates
	some of the performance gain.  We copy  Metadata and then alter nsamp
	in the copy before calling the constructor.   The code that seg faulted
	just did a dynamic cast to the input, which created the problem.
	*/
	Metadata mdtmp(dynamic_cast<const Metadata&>(parent));
	mdtmp.put_long(SEISMICMD_npts,outns);
	Seismogram result(btstmp,mdtmp);

	// Perhaps should do this with blas or memcpy for efficiency
  //  but this makes the algorithm much clearer
  int i,ii,k;
  for(i=is,ii=0;i<=ie && i<parent.npts() && ii<outns;++i,++ii)
      for(k=0;k<3;++k)
      {
          result.u(k,ii)=parent.u(k,i);
      }
	/* Necessary because the constructor we called will set the Seismogram
	it creates dead.  After filling in data we can mark it live */
	result.set_live();
	/*This dynamic_cast may not be necessary, but makes the api clear */
	result.load_history(dynamic_cast<const ProcessingHistory&>(parent));
	return result;
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
  /* This calculation was used in earlier versions but was found to 
     differ by 1 depending on the subsample timing of t0 of parent.  the 
     problem is that sample_number uses rounding which can produce that
     effect due to a subtle interaction with tw.start and tw.end 
     relative to the sample grid. 
  int ie=parent.sample_number(tw.end);
  */
  int outns,ie;
  outns = round((tw.end-tw.start)/parent.dt()) + 1;  
  ie = is + outns - 1;
	//Ridiculous (int) case to silence a bogus compiler warning
  if( (is<0) || (ie>((int)parent.npts())) )
  {
      ostringstream mess;
          mess << "WindowData(TimeSeries):  Window mismatch"<<endl
              << "Window start time="<<tw.start<< " is sample number "
              << is<<endl
              << "Window end time="<<tw.end<< " is sample number "
              << ie<<endl
              << "Parent has "<<parent.npts()<<" samples"<<endl;
			/* Making this copy is a bit inefficient, but necessary unless we
			wanted to drop the const qualifier onparent*/
			TimeSeries dret(parent);
			dret.kill();
			/* Here appropriate to clear the data array  - no reason to carry
			around baggages */
			dret.set_npts(0);
			dret.elog.log_error("WindowData",mess.str(),ErrorSeverity::Invalid);
			return dret;
  }
	/* MAINTENANCE ISSUE:  The original implementation of this code used a copy constructor
	here to initalize result.   We realized that was very inefficient when
	slicing down long input signals like data blocked in day files.
	We converted to this algorithm BUT be warned it is somewhat fragile
	as it depends on some side effects of the implementation of TimeSeries
	that could break it if the following assumptions changes:
	1.  The constructor we use calls the set_npts method
	2.  that constructor also syncs npts metadata
	3.  The constructor initializes the std::vector to zeros with length
	    determined from the set_npts result.
	Less important is we have to add the load_history call here or history
	would be lost.  Reason is the constructor uses CoreTimeSeries. */

	BasicTimeSeries btstmp(dynamic_cast<const BasicTimeSeries&>(parent));
	btstmp.set_npts(outns);
  /* Using the time method here preserves subsample timing.*/
	btstmp.set_t0(parent.time(is));
	TimeSeries result(btstmp,dynamic_cast<const Metadata&>(parent));
	/* That constuctor initalizes s to zeroes so we can copy directly
	to the container without push_back.  memcpy might buy a small performance
	gain but would make this more fragile that it already is. */
  int i,ii;
	for(i=is,ii=0;i<=ie && i<parent.npts() && ii<outns;++i,++ii) 
    result.s[ii] = parent.s[i];
	/*This dynamic_cast may not be necessary, but makes the api clear */
	result.load_history(dynamic_cast<const ProcessingHistory&>(parent));

  return(result);
}
} // end mspass namespace
