#include <math.h>
#include "mspass/algorithms/TimeWindow.h"
#include "mspass/utility/MsPASSError.h"
#include "mspass/utility/SphericalCoordinate.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/Ensemble.h"
#include "mspass/seismic/keywords.h"
namespace mspass::algorithms
{
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;

/* This file contains helper procedures for Seismogram objects.  Most
are truly procedural and take a Seismogram object or a Ensemble
object and return one or the other.

This file was modified from code developed earlier in antelope contrib.   Most
changes are little more than name changes.

Author:  Gary L. Pavlis
	Indiana University
	pavlis@indiana.edu
*/


/* This function converts a Seismogram from an absolute time standard to
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

shared_ptr<Seismogram> ArrivalTimeReference(Seismogram& tcsi,
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
                          + string(" not found in Seismogram object"),
                          ErrorSeverity::Invalid);
    }
    // We have to check this condition because ator will do nothing if
    // time is already relative and this condition cannot be tolerated
    // here as we have no idea what the time standard might be otherwise
    if(tcsi.time_is_relative())
        throw MsPASSError(string("ArrivalTimeReference:  ")
                          + string("received data in relative time units\n")
                          + string("Cannot proceed as timing is ambiguous"),
                          ErrorSeverity::Invalid);
    // start with a clone of the original
    shared_ptr<Seismogram> tcso(new Seismogram(tcsi));
    tcso->ator(atime);  // shifts to arrival time relative time reference
    // Simply return a copy of traces marked dead
    if(tcso->dead()) return(tcso);
    /* this section does nothing if the time window requested exceeds the
    data extent, BUT ensembles of such data will have irregular start and
    end times.  Presume all code will handle that case as the norm in
    passive array data, but might be confusing if applied to active source
    data.  */
    if( (tw.start > tcso->t0())  || (tw.end<tcso->time(tcso->u.columns()-1) ) )
    {
        int jstart, jend;
        int ns_to_copy;
        int i,j,jj;
        jstart = tcso->sample_number(tw.start);
        jend = tcso->sample_number(tw.end);
        if(jstart<0) jstart=0;
        if(jend>=tcso->u.columns()) jend = tcso->u.columns() - 1;
        ns_to_copy = jend - jstart + 1;
        // This is a null trace so mark it dead in this condition
        if(ns_to_copy<0)
        {
            ns_to_copy=0;
            tcso->kill();
            tcso->u=dmatrix(1,1);
            tcso->set_npts(0);
        }
        else
        {
            // This is not the fastest way to do this, but it is
            // clearer and the performance hit should not be serious
            // old advice:  make it work before you make it fast
            // this is not needed in new api because set_npts does this
            //tcso->u = dmatrix(3,ns_to_copy);
            tcso->set_npts(ns_to_copy);
            for(i=0; i<3; ++i)
                for(j=0,jj=jstart; j<ns_to_copy; ++j,++jj)
                    tcso->u(i,j)=tcsi.u(i,jj);
            //old api form
            //tcso->t0 += (tcso->dt)*static_cast<double>(jstart);
            double newt0;
            newt0=tcso->t0() + (tcso->dt())*static_cast<double>(jstart);
            tcso->set_t0(newt0);
            /* Don't think the block below is needed for new api, but will retain this
            code commented out until I know for sure glp-6/7/2020*/
            // start of old
            //
            // This is necessary to allow for a rtoa (relative
            // to absolute time) conversion later.
            //
            /*
            if(jstart>0)
            {
                double stime=atime+tcso->t0();
                tcso->put("time",stime);
                // this one may not really be necessary
                tcso->put("endtime",atime+tcso->endtime());
            }
            end of old */
        }
    }
    return(tcso);
}
/* Similar function to above but processes an entire ensemble.  The only
special thing it does is handle exceptions.  When the single object
processing function throws an exception the error is printed and the
object is simply not copied to the output ensemble */
shared_ptr<Ensemble<Seismogram>> ArrivalTimeReference(Ensemble<Seismogram>& tcei,
        string arrival_time_key,
        TimeWindow tw)
{
    int nmembers=tcei.member.size();
    // use the special constructor to only clone the metadata and
    // set aside slots for the new ensemble.
    shared_ptr<Ensemble<Seismogram>>
    tceo(new Ensemble<Seismogram>(dynamic_cast<Metadata&>(tcei),
                                    nmembers));
    tceo->member.reserve(nmembers);  // reserve this many slots for efficiency
    // We have to use a loop instead of for_each as I don't see how
    // else to handle errors cleanly here.
    vector<Seismogram>::iterator indata;
    for(indata=tcei.member.begin(); indata!=tcei.member.end(); ++indata)
    {
        try {
            shared_ptr<Seismogram>
            tcs(ArrivalTimeReference(*indata,arrival_time_key,tw));
            tceo->member.push_back(*tcs);
        } catch ( MsPASSError& serr)
        {
            serr.log_error();
            cerr << "This Seismogram was not copied to output ensemble"<<endl;
        }
    }
    return(tceo);
}

/* This is a procedural form of one of the rotate method and is a bit redundant. */
void HorizontalRotation(Seismogram& d, double phi)
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

TimeSeries ExtractComponent(const Seismogram& tcs,const unsigned int component)
{
  /* No need to test for negative because we use an unsigned */
  if(component>=3) throw MsPASSError("ExtractComponent: illegal component number - must be 0, 1, or 2",
      ErrorSeverity::Invalid);
  /* Return a null seismogram if tcs is marked dead */
  if(tcs.dead()) return TimeSeries();
  try{
    vector<double> scomp;
    for(size_t i=0;i<tcs.npts();++i)
    {
      scomp.push_back(tcs.u(component,i));
    }
    TimeSeries result(dynamic_cast<const BasicTimeSeries&>(tcs),
      dynamic_cast<const Metadata&>(tcs),
        dynamic_cast<const ProcessingHistory&>(tcs),
          scomp);
    /* This section insert hang and vang.  We use the SphericalCoordinate
    function to convert the unit vector stored in the rows of the tmatrix. */
    double nu[3],hang,vang;
    if(tcs.cardinal())
    {
      /* We handle this case under an assumption it is worth avoiding the
      cost of the trig functions needed to handle the not cardinal case */
      switch(component)
      {
        case 0:
          hang=90.0;
          vang=90.0;
          break;
        case 1:
          hang=0.0;
          vang=90.0;
          break;
        case 2:
          hang=0.0;
          vang=0.0;
      };
    }
    else
    {
      dmatrix tm=tcs.get_transformation_matrix();
      for(size_t k=0;k<3;++k)
      {
        nu[k]=tm(component,k);
      }
      SphericalCoordinate sc;
      sc=UnitVectorToSpherical(nu);
      vang=mspass::utility::deg(sc.theta);
      hang=90.0-mspass::utility::deg(sc.phi);
    }
    result.put(SEISMICMD_hang,hang);
    result.put(SEISMICMD_vang,vang);

    /*we hard code some ProcessingHistory here with the algid defining the
    component number extracted.   This model should work for any algorithm that
    has only one argument that can be represented as a string.  We do do
    nothing of the history is empty. */
    if(!result.is_empty())
    {
      stringstream ss;
      ss<<"component="<<component;
      result.new_map("ExtractComponent",ss.str(),AtomicType::TIMESERIES,
         ProcessingStatus::VOLATILE);
    }
    return result;
  }catch(...){throw;};
}
Ensemble<TimeSeries> ExtractComponent(const Ensemble<Seismogram>& d,
		const unsigned int comp)
{
  //Using size_t = unsigned int means we don't need to check negative comp
  if(comp>=3)
    throw MsPASSError("ExtractComponent(Ensmble): illegal component number - must be 0,1, or 2",
		    ErrorSeverity::Invalid);
  try{
    Ensemble<TimeSeries> result;
    result.Metadata::operator=(d);
    result.member.reserve(d.member.size());
    vector<Seismogram>::const_iterator dptr;
    for(dptr=d.member.begin();dptr!=d.member.end();++dptr)
    {
      TimeSeries dts;
      dts=ExtractComponent(*dptr,comp);
      result.member.push_back(dts);
    }
    return result;
  } catch(...){throw;};
};


} // end mspass namespace encapsulation
