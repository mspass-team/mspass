#include <algorithm>
#include "mspass/utility/MsPASSError.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/Ensemble.h"
#include "mspass/algorithms/algorithms.h"
namespace mspass::algorithms
{
using mspass::seismic::BasicTimeSeries;
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
/* This small helper with a rather colorful name describes its function.
When bundling fails we need a way to id the body.   We do that here by
merging all the Metadata with the Metadata += operator.   That will leave
some relics of the order, but should allow the user to id the body */
Seismogram dogtag(vector<CoreTimeSeries>& bundle)
{
  Metadata md;
  size_t i(0);
  for(auto d=bundle.begin();d!=bundle.end();++d,++i)
  {
    if(i==0)
    {
      md=dynamic_cast<Metadata&>(*d);
    }
    else
    {
      md += dynamic_cast<Metadata&>(*d);
    }
  }
  Seismogram result(0);
  /* this very obscure syntax calls operator = for Metadata which copies
  only md to the result. */
  result.Metadata::operator=(md);
  result.kill();
  return result;
}
/* This function is useful standalone as a way o bundle data assembled
from reading TimeSeries from MongoDB.  We use it below to handle ensemble
groupings that are irregular but it has broader use provided the data
are from miniseed and have chan defined.    */
Seismogram BundleGroup(std::vector<TimeSeries>& d,
  const size_t i0, const size_t iend)
{
  const string algname("BundleGroup");
  Seismogram result;
  try{
    string chan;
    /* We load both a vector and set container with the chan keys.
    Depends on set inserts overwrite so the set container will only have
    unique keys while the vector has the chan for each vector component */
    vector<string> chans_this_group;
    set<string> keys;
    for(size_t i=i0;i<=iend;++i)
    {
      chan=d[i].get_string("chan");
      chans_this_group.push_back(chan);
      keys.insert(chan);
    }
    size_t nkeys=keys.size();
    vector<CoreTimeSeries> bundle;
    vector<ProcessingHistory*> hvec;
    if(chans_this_group.size()<=3)
    {
      /* We can handle deficient groups here by this algorithm.
      After conditionals we kill output when size is less than 3.*/
      bundle.reserve(chans_this_group.size());
      for(size_t i=i0;i<=iend;++i)
      {
        bundle.push_back(dynamic_cast<CoreTimeSeries&>(d[i]));
        if( ! (d[i].is_empty()) )
        {
          hvec.push_back(dynamic_cast<ProcessingHistory*>(&d[i]));
        }
      }
    }
    else
    {
      /* We use this multimap to sort out duplicates */
      multimap<string,size_t> xref;
      /* We use this for handling find returns from xref */
      multimap<string,size_t>::iterator xptr;
      size_t i,ii;
      for(i=i0,ii=0;i<=iend;++i,++ii)
      {
        chan=chans_this_group[ii];
        keys.insert(chan);
        xref.insert(pair<string,size_t>(chan,i));
      }
      /* Only when number of keys is 3 can we hope to form a valid bundle.
      This first block does that resolving duplicates by the channel with
      the closest match to the start time defined by minimum time of the group*/

      if(nkeys==3)
      {
        for(auto kptr=keys.begin();kptr!=keys.end();++kptr)
        {
          /* Note we don't have to test for no match because it isn't possible*/
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
            /* We use the algorithm described in the include file doxygen
            docs.   Briefly find the minimum time of this entire group and
            use that as an anchor.  component with t0 closest to that time
            is selected.  So first get min t0*/
            double t0min=d[i0].t0();
            for(i=i0+1;i<=iend;++i)
            {
              if(d[i].t0()<t0min) t0min=d[i].t0();
            }
            pair<multimap<string,size_t>::iterator,multimap<string,size_t>::iterator> ret;
            ret=xref.equal_range(*kptr);
            size_t j(0),jjmin(0),jj;
            double dt,dtmin;
            for(xptr=ret.first;xptr!=ret.second;++xptr,++j)
            {
              jj=xptr->second;
              if(j==0)
              {
                jjmin=jj;
                dtmin=fabs(d[jj].t0()-t0min);
              }
              else
              {
                dt=fabs(d[jj].t0()-t0min);
                if(dt<dtmin)
                {
                  jjmin=jj;
                  dtmin=dt;
                }
              }
            }
            bundle.push_back(dynamic_cast<CoreTimeSeries&>(d[jjmin]));
            if( ! (d[jjmin].is_empty()) )
            {
              hvec.push_back(dynamic_cast<ProcessingHistory*>(&d[jjmin]));
            }
          }
        }
      }
      else
      {
        /* We handle deficient and excess keys identically in terms of
        the loop below.  We split them up for different error messages
        when we try to create a seismogram below */
        for(size_t i=i0;i<=iend;++i)
        {
          bundle.push_back(dynamic_cast<CoreTimeSeries&>(d[i]));
          if( ! (d[i].is_empty()) )
          {
            hvec.push_back(dynamic_cast<ProcessingHistory*>(&d[i]));
          }
        }
      }
    }
    Seismogram d3c;
    if(nkeys==3)
    {
      try{
        d3c=Seismogram(CoreSeismogram(bundle),algname);
        if(hvec.size()==3) d3c.add_many_inputs(hvec);
      }
      catch(MsPASSError& err)
      {
        d3c=dogtag(bundle);
        if(hvec.size()>0) d3c.add_many_inputs(hvec);
        d3c.kill();
        d3c.elog.log_error(err);
      };
    }
    else
    {
      d3c=dogtag(bundle);
      if(hvec.size()>0) d3c.add_many_inputs(hvec);
      d3c.kill();
      stringstream ss;
      if(nkeys<3)
      {
        ss<<"Insufficient data to generate Seismogram object."<<endl
          <<"Number of channels received  for bundling="<<bundle.size()<<endl
          <<"Number of unique names in group="<<nkeys<<endl
          <<"Number of unique names must be exactly 3"<<endl;
      }
      else
      {
        ss<<"Excess channel keys in group received"<<endl
          <<"Number of unique channel names="<<nkeys<<" which should be 3"<<endl
          << "Channel names received: ";
        for(auto chanptr=chans_this_group.begin();
                chanptr!=chans_this_group.end();++chanptr) ss<<(*chanptr)<<" ";
        ss<<endl;
      }
      d3c.elog.log_error("BundleGroup",ss.str(),ErrorSeverity::Invalid);
    }
    return d3c;
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
              Seismogram d3c;
              try{
                d3c=Seismogram(CoreSeismogram(work),algname);
                if(hvec.size()==3) d3c.add_many_inputs(hvec);
              }catch(MsPASSError& err)
              {
                d3c=dogtag(work);
                if(hvec.size()>0) d3c.add_many_inputs(hvec);
                d3c.kill();
                d3c.elog.log_error(err);
              }
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
              Seismogram dgrp(BundleGroup(d.member,i0,iend));
              ens3c.member.push_back(dgrp);
            }
          }
          else
          {
            /* we land here for deficient groups.   We create a body bag
            with the dogtag function and handle the components from this
            case exactly like that when the Seismogram constructor
            above throws an exception */
            vector<CoreTimeSeries> work;
            vector<ProcessingHistory*> hvec;
            for(size_t k=i0;k<=iend;++k)
            {
              work.push_back(dynamic_cast<CoreTimeSeries&>(d.member[k]));
              if( ! (d.member[k].is_empty()) )
              {
                hvec.push_back(dynamic_cast<ProcessingHistory*>(&d.member[k]));
              }
            }
            Seismogram d3c;
            d3c=dogtag(work);
            if(hvec.size()>0) d3c.add_many_inputs(hvec);
            d3c.kill();
            stringstream ss;
            ss<<"Insufficient data to generate Seismogram object."<<endl
              <<"Number of channels received  for bundling="<<work.size()<<endl
              <<"Should be three but can sometimes recover with greater than 3"<<endl;
            d3c.elog.log_error("bundle_seed_data",ss.str(),ErrorSeverity::Invalid);
            ens3c.member.push_back(d3c);
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
/* This one liner is a useful as a processing function and is thus
exposed to python.  */
void seed_ensemble_sort(Ensemble<TimeSeries>& d)
{
  try{
    std::sort(d.member.begin(),d.member.end(),greater_seedorder());
  }catch(...){throw;};
}
}  // End namespace
