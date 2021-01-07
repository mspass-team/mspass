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
enum SortKey {NET,STA,CHAN,LOC};
/*! /brief Function object used for sorting with metadata key.

The stl sort algorithm can use a general function to define order.  This template
allows sorting a vector of objects containing the SortKey using the
generic stl sort algorithm.

\param T is the ensemble class type
\param SO is a SortOrder enum value that is used to define the attribute for
        sorting.  This has to be an enum as I've found no way to give a function
        object like this a parameter any other way.
*/
template <typename Tdata, SortKey KEY,typename Tkey=std::string> struct greater_metadata
                : public binary_function<Tdata,Tdata,bool>
{
  bool operator()(Tdata x, Tdata y)
  {
    string key;
    switch(KEY)
    {
      case STA:
        key="sta";
        break;
      case CHAN:
        key="chan";
        break;
      case LOC:
        key="loc";
        break;
      case NET:
      default:
        key="net";
    };
    Tkey valx,valy;
    /* This seems necessary because of the namespace complications.
       Ancestor of this function in XcorProcessingEngine didn't require
       this complexity.  There may be another way to do this - the compiler
       suggests a "use 'template' keyword" but I'll us this 
    */
    mspass::utility::Metadata *xptr,*yptr;
    xptr=dynamic_cast<Metadata*>(&x);
    yptr=dynamic_cast<Metadata*>(&y);
    try{
      valx=xptr->get<Tkey>(key);
    }catch(...){return true;};
    try{
      valy=yptr->get<Tkey>(key);
    }catch(...){return false;};
    if(valx>valy)
      return true;
    else
      return false;
  }
};
/* Helper for below.  Returns a pair of iterators than can be passed to
sort.  */
pair<vector<TimeSeries>::iterator,vector<TimeSeries>::iterator>
  find_range(vector<TimeSeries>::iterator anchor,
    vector<TimeSeries>::iterator end_of_data, string key)
{
  string testval=anchor->get<string>(key);
  vector<TimeSeries>::iterator dptr,dlast;
  for(dptr=anchor;dptr!=end_of_data;++dptr)
  {
    string thisval=dptr->get<string>(key);
    if(thisval!=testval)
    {
      /* Just calling break works because we set dlast.  Behaves the same
      that way if the for loop goes to its end. */
      break;
    }
    dlast=dptr;
  }
  return pair<vector<TimeSeries>::iterator,vector<TimeSeries>::iterator>
        (anchor,dlast);
}

void seed_sort(Ensemble<TimeSeries>& d)
{
  const string base_error("bundle_seed_data:  ");
  /* This algorithm uses four string keys used in seed to define a unique
  seismic channel.   We first do a safety check to validate metadata.
  We require:
  1.  net is all or none.  If no members have net set the net sort is just
      bypassed. What we can't allow is some set and some not set.
  2.  sta and chan are dogmatically required to be set in every member
  3.  loc is treated like net.  It must be all or none
  */
  bool use_net,use_loc,found_null(false),found_set(false);
  vector<TimeSeries>::iterator dptr;
  for(dptr=d.member.begin();dptr!=d.member.end();++dptr)
  {
    if(dptr->live())
    {
      if(dptr->is_defined("net"))
        found_set=true;
      else
      {
        found_null=true;
        break;   //any null requires a bypass
      }
      /* As soon as we find a mismatch we need to abort */
      if(found_null && found_set)
      {
        throw MsPASSError(base_error+"ensemble has net set of some but not all members",
            ErrorSeverity::Invalid);
      }
    }
  }
  if(found_set)
    use_net=true;
  else
    use_net=false;
  /* Same test for loc */
  found_null=false;
  found_set=false;
  for(dptr=d.member.begin();dptr!=d.member.end();++dptr)
  {
    if(dptr->live())
    {
      if(dptr->is_defined("loc"))
        found_set=true;
      else
      {
        found_null=true;
        break;
      }
      /* As soon as we find a mismatch we need to abort */
      if(found_null && found_set)
      {
        throw MsPASSError(base_error+"ensemble has loc set of some but not all members",
            ErrorSeverity::Invalid);
      }
    }
  }
  if(found_set)
    use_loc=true;
  else
    use_loc=false;
  for(dptr=d.member.begin();dptr!=d.member.end();++dptr)
  {
    if(dptr->live())
    {
      if(!dptr->is_defined("sta"))
      {
        throw MsPASSError(base_error+"ensemble has a live member without sta set",
          ErrorSeverity::Invalid);
      }
      if(!dptr->is_defined("chan"))
      {
        throw MsPASSError(base_error+"ensemble has a live member without chan set",
          ErrorSeverity::Invalid);
      }
    }
  }
  /* Now we go through a staged sort in the order net, sta, loc, chan.
  loc is treated differently to allow null loc codes */
  if(use_net)
  {
    sort(d.member.begin(),d.member.end(),
        greater_metadata<TimeSeries,NET,std::string>());
  }
  pair< vector<TimeSeries>::iterator, vector<TimeSeries>::iterator> range;
  /* We have to do next sorts in groups. There is probably a way to do this
  with stl algorithms but I use this hand rolled function defind above*/
  vector<TimeSeries>::iterator dptr0;  /* Anchor for forward searches */
  dptr0=d.member.begin();
  /* A bit of a weird loop that only terminates on the break but it is the
  correct logic*/
  while(true)
  {
    range=find_range(dptr0,d.member.end(),string("sta"));
    sort(range.first,range.second,greater_metadata<TimeSeries,STA,std::string>());
    dptr0=range.second;
    if(dptr0==d.member.end()) break;
    ++dptr0;
  }
  if(use_loc)
  {
    while(true)
    {
      range=find_range(dptr0,d.member.end(),string("loc"));
      sort(range.first,range.second,greater_metadata<TimeSeries,LOC,std::string>());
      dptr0=range.second;
      if(dptr0==d.member.end()) break;
      ++dptr0;
    }
  }
  /* Final sort is by chan.  The seed standard requires chan names to sort
  cleanly for 3c bundles.  e.g. mixing BHZ, BHN, BHE, HHZ, HHN, and HHE yields
  the order BHE, BHN, BHZ, HHE, HHN, HHZ.*/
  while(true)
  {
    range=find_range(dptr0,d.member.end(),string("chan"));
    sort(range.first,range.second,greater_metadata<TimeSeries,CHAN,std::string>());
    dptr0=range.second;
    if(dptr0==d.member.end()) break;
    ++dptr0;
  }
}
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
    seed_sort(d);
    /* this constructor clones the ensemble metadata */
    Ensemble<Seismogram> ens3c(dynamic_cast<Metadata&>(d),d.member.size()/3);
    vector<TimeSeries>::iterator dptr;
    bool use_net(false),use_loc(false);
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
          use_net=true;
          lastnet=dptr->get<string>("net");
        }
        if(dptr->is_defined("loc"))
        {
          use_loc=true;
          lastloc=dptr->get<string>("loc");
        }
        laststa=dptr->get<string>("sta");
        lastchan=dptr->get<string>("chan");
        i0=0;
      }
      else
      {
        if(use_net)
        {
          net=dptr->get<string>("net");
        }
        else
        {
          net="";
        }
        if(use_loc)
        {
          loc=dptr->get<string>("loc");
        }
        else
        {
          loc="";
        }
        sta=dptr->get<string>("sta");
        chan=dptr->get<string>("chan");
        if( (lastnet!=net || laststa!=sta || lastloc!=loc) ||
               (dptr==d.member.end()) )
        {
          iend=i-1;
          /*Silently drop incomplete groups */
          if((i-i0)>3)
          {
            if((i-i0) == 3)
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
        }
      }
    }
    return ens3c;
  }catch(...){throw;};
}
}  // End namespace
