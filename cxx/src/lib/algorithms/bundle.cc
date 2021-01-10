#include <algorithm>
#include "mspass/utility/MsPASSError.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/Ensemble.h"
#include "mspass/algorithms/algorithms.h"
namespace mspass::algorithms
{
using mspass::seismic::CoreTimeSeries;
using mspass::seismic::CoreSeismogram;
using mspass::seismic::TimeSeries;
using mspass::seismic::Seismogram;
using mspass::seismic::Ensemble;
using mspass::seismic::TimeReferenceType;
using mspass::utility::Metadata;
using mspass::utility::ProcessingHistory;
using mspass::utility::MsPASSError;
using mspass::utility::ErrorSeverity;

using namespace std;

/* Cautious comparison of two string metadata fields defined by key.\
Return -1 if x<y, 0 if x==y, and +1 if x>y.

Undefined values require a definition.   two undefined field compare equal but
and undefined value always compare > a defined field. */

int cautious_compare(const TimeSeries& x, const TimeSeries& y, const string key)
{
  if(x.is_defined(key))
  {
    if(y.is_defined(key))
    {
      string valx,valy;
      valx=x.get<string>(key);
      valy=y.get<string>(key);
      if(valx>valy)
        return 1;
      else if(valx==valy)
        return 0;
      else
        return -1;
    }
    else
    {
      /* x_defined and y_undefined here - x_defined >y_undefined*/
      return 1;

    }
  }else if(y.is_defined(key))
  {
    /* x_undefined and y_defined - x_undefined < y_defined */
    return -1;
  }
  else
  {
    /* Both undefined - weak order requires return false. */
    return 0;
  }
}
struct greater_seedorder
{
  /* Note this sort so undefined will be less than any defined value.
  Could be the reverse but an arbitrary choice */
  bool operator()(TimeSeries x, TimeSeries y)
  {
    int retnet,retsta,retloc,retchan;
    retnet=cautious_compare(x,y,"net");
    switch(retnet)
    {
      case -1:
        return false;
      case +1:
        return true;
      case 0:
      default:
        retsta=cautious_compare(x,y,"sta");
        switch(retsta)
        {
          case -1:
            return false;
          case +1:
            return true;
          case 0:
          default:
            retloc=cautious_compare(x,y,"loc");
            switch(retloc)
            {
              case -1:
                return false;
              case +1:
                return true;
              case 0:
              default:
                retchan=cautious_compare(x,y,"chan");
                switch(retchan)
                {
                  case +1:
                    return true;
                  default:
                    /* Weak order as the final condition is strictly > so
                    equal case only here returns false */
                    return false;
                };
            };
        };
    };
  };
};


/* This function is useful standalone as a way o bundle data assembled
from reading TimeSeries from MongoDB.  We use it below to handle ensemble
groupings that are irregular.   The algorithm has three fundamental assumption s
that is guaranteed when called from bundle_seed_data below, but could
be problem if used in isolation.   python bindings should contain
validation to guaranteed input those function called directly from python
satisfies these two assumpions:
1.  chan is set for all data and is a valid seed channel code.
2.  The input has been sorted by chan
3.  All members of hte bundle received have approximately the same start time.

The algorithm used to handle irregular bundles is not elegant and will fail
if th reason the in put is irregular (i.e. not a mulitple of 3) is a
data gap that cause something upstream to fragment the data into pieces.
Basically the algorithm used works only to handle duplicates.  It will
silenetly drop the duplicates if tha common start time assumption is true.
It is subject to throwing exceptions by th CoreSeismogram constructor
that assembles thes three componnts. That mainly means the sample rates
must match and obvious metadata like hang and vang are required. */

vector<Seismogram> BundleGroup(std::vector<TimeSeries>& d,
  const size_t i0, const size_t iend)
{
  const string algname("BundleGroup");
  vector<Seismogram> result;
  try{
    /* We use the first two characters in chan names to define groupings.
    These two strings hold an anchor for testing and current substring in
    the scan loop.  The algorithm will only work if the ensemble was
    previously sorted alphabetically by channel code and the codes obey
    seed rules. We also don't test that chan exists because in this file
    usage the sort above will fail if chan is not defined.   Do not
    transport this code without accounting for that assumption*/
    string ss0,ss;
    size_t ig,i0g0;
    string chan;
    vector<string> chans_this_group;
    chan=d[i0].get_string("chan");
    ss0.assign(chan,0,2);
    for(ig=i0,i0g0=i0;ig<=iend;++ig)
    {
      chan=d[ig].get_string("chan");
      ss.assign(chan,0,2);
      if(ss==ss0 && ig!=iend)
      {
        chans_this_group.push_back(chan);
      }
      else
      {
        /* This is and end condition to handle break at end of range */
        size_t igend;
        if(ig==iend)
          igend=iend+1;
        else
          igend=ig;
        vector<CoreTimeSeries> bundle;
        vector<ProcessingHistory*> hvec;
        if(chans_this_group.size()==3)
        {
          bundle.reserve(3);
          for(auto k=i0g0;k<igend;++k)
          {
            bundle.push_back(dynamic_cast<CoreTimeSeries&>(d[k]));
            if( ! (d[k].is_empty()) )
            {
              hvec.push_back(dynamic_cast<ProcessingHistory*>(&d[k]));
            }
          }
        }
        else
        {
          //Handle irregular data here.  Create a simple vector with only 3
          /* We use this multimap to sort out duplicates */
          multimap<string,size_t> xref;
          /* We use this for handling find returns from xref */
          multimap<string,size_t>::iterator xptr;
          /* We need this because a multimap doesn't have a unique keys method */
          set<string> keys;

          for(size_t k=i0g0;k<iend;++k)
          {
            chan=d[k].get_string("chan");
            keys.insert(chan);
            xref.insert(pair<string,size_t>(chan,k));
          }
          /*Discard this group if the number of unique keys is exactly 3*/
          if(keys.size()!=3) continue;
          for(auto kptr=keys.begin();kptr!=keys.end();++kptr)
          {
            if(xref.count(*kptr)==1)
            {
              xptr=xref.find(*kptr);
              bundle.push_back(dynamic_cast<CoreTimeSeries&>
                      (d[xptr->second]));
              if( ! (d[xptr->second].is_empty()) )
              {
                hvec.push_back(dynamic_cast<ProcessingHistory*>
                        (&(d[xptr->second])));
              }
            }
            else
            {
              /* Could do something more sophisticated here but for now just
              grab the first entry assuming this is only a duplication problem.*/
              xptr=xref.find(*kptr);
              size_t inow=xptr->second;
              bundle.push_back(dynamic_cast<CoreTimeSeries&>(d[inow]));
              if( ! (d[inow].is_empty()) )
              {
                hvec.push_back(dynamic_cast<ProcessingHistory*>(&d[inow]));
              }
            }
          }
        }
        Seismogram d3c(CoreSeismogram(bundle),algname);
        if(hvec.size()==3) d3c.add_many_inputs(hvec);
        result.push_back(d3c);
      }
    }
    return result;
  }catch(...){throw;};
}
/* Warning this function will alter the order of the ensemble.  A const
version could be written but it would need to copy d before calling the sort*/
Ensemble<Seismogram> bundle_seed_data(Ensemble<TimeSeries>& d)
{
  string algname("bundle_seed_data");
  try{
    std::sort(d.member.begin(),d.member.end(),greater_seedorder());
    //Debug
    /*
    for(auto dtmp=d.member.begin();dtmp!=d.member.end();++dtmp)
    {
      cout << dtmp->get<string>("net")<<" "
        << dtmp->get<string>("sta")<<" "
        << dtmp->get<string>("loc")<<" "
        << dtmp->get<string>("chan")<<endl;
    }
    */
    /* this constructor clones the ensemble metadata */
    Ensemble<Seismogram> ens3c(dynamic_cast<Metadata&>(d),d.member.size()/3);
    vector<TimeSeries>::iterator dptr;
    string laststa,lastloc,lastchan,lastnet;
    string net(""),sta,chan,loc("");
    size_t i0,iend;
    size_t i;
    for(i=0,dptr=d.member.begin();dptr!=d.member.end();++i,++dptr)
    {
      if(dptr->dead()) continue;
      if(i==0)
      {
        /* These things need to all be initialized by the first member*/
        if(dptr->is_defined("net"))
        {
          lastnet=dptr->get<string>("net");
        }
        else
        {
          lastnet="Undefined";
        }
        if(dptr->is_defined("loc"))
        {
          lastloc=dptr->get<string>("loc");
        }
        else
        {
          lastloc="Undefined";
        }
        laststa=dptr->get<string>("sta");
        lastchan=dptr->get<string>("chan");
        i0=0;
      }
      else
      {
        if(dptr->is_defined("net"))
        {
          net=dptr->get<string>("net");
        }
        else
        {
          net="Undefined";
        }
        if(dptr->is_defined("loc"))
        {
          loc=dptr->get<string>("loc");
        }
        else
        {
          loc="Undefined";
        }
        sta=dptr->get<string>("sta");
        chan=dptr->get<string>("chan");
        if( (lastnet!=net || laststa!=sta || lastloc!=loc) ||
               (dptr==(d.member.end() - 1)) )
        {
          /* The end condition has to be handled specially because when
          we reach the end we have attempt a bundle.   Inside the ensemble
          the test is against the previous values of the seed keys*/
          if(dptr==(d.member.end()-1))
            iend=i;
          else
            iend=i-1;
          /*Silently drop incomplete groups */
          if((iend-i0)>=2)  //Use iend to handle end condition instead of i
          {
            if((iend-i0) == 2)
            {
              vector<CoreTimeSeries> work;
              vector<ProcessingHistory*> hvec;
              work.reserve(3);
              for(size_t k=0;k<3;++k)
              {
                work.push_back(dynamic_cast<CoreTimeSeries&>(d.member[i0+k]));
                if( ! (d.member[i0+k].is_empty()) )
                {
                  hvec.push_back(dynamic_cast<ProcessingHistory*>(&d.member[i0+k]));
                }
              }
              Seismogram d3c(CoreSeismogram(work),algname);
              if(hvec.size()==3) d3c.add_many_inputs(hvec);
              ens3c.member.push_back(d3c);
            }
            else
            {
              /* We put this in a function because there is a fair amount of
              complexity in sorting out channel groups better kept together.
              This main function is already complicated enough.  The
              function returns a vector of CoreSeismograms using a grouping
              that depends upon seed naming conventions for channels.
              */
              vector<Seismogram> dgrp;
              dgrp=BundleGroup(d.member,i0,iend);
              for(auto k=0;k<dgrp.size();++k) ens3c.member.push_back(dgrp[k]);
            }
          }
          laststa=sta;
          lastnet=net;
          lastloc=loc;
          lastchan=chan;
          i0=i;
        }
      }
    }
    return ens3c;
  }catch(...){throw;};
}
}  // End namespace
