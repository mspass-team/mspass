#include <string>
#include <algorithm>
#include "mspass/utility/MsPASSError.h"
#include "mspass/seismic/keywords.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/Ensemble.h"
#include "mspass/algorithms/algorithms.h"
namespace mspass::algorithms
{
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;


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
    retnet=cautious_compare(x,y,SEISMICMD_net);
    switch(retnet)
    {
      case -1:
        return false;
      case +1:
        return true;
      case 0:
      default:
        retsta=cautious_compare(x,y,SEISMICMD_sta);
        switch(retsta)
        {
          case -1:
            return false;
          case +1:
            return true;
          case 0:
          default:
            retloc=cautious_compare(x,y,SEISMICMD_loc);
            switch(retloc)
            {
              case -1:
                return false;
              case +1:
                return true;
              case 0:
              default:
                retchan=cautious_compare(x,y,SEISMICMD_chan);
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
Seismogram BundleSEEDGroup(const std::vector<TimeSeries>& d,
  const size_t i0, const size_t iend)
{
  const string algname("BundleSEEDGroup");
  try{
    vector<CoreTimeSeries> bundle;
    vector<ProcessingHistory*> hvec;
    /* I had to force this alias for the input d from a const cast to
    allow the dynamic casting below to work for ProcessingHistory.  Unclear
    why but I think it is a limitation of std::vector.  This is a workaround*/
    std::vector<TimeSeries> d_const;
    d_const=const_cast<std::vector<TimeSeries>&>(d);
    Seismogram d3c;  // this holds the result even if marked dead
    string net,sta,chan,loc;
    /*this holds the first 2 characters in the chan code that seed
    from data centers is always constant for a 3c set.*/
    string sta2;
    /* We load both a vector and set container with the chan keys.
    Depends on set inserts overwrite so the set container will only have
    unique keys while the vector has the chan for each vector component */
    vector<string> chans_this_group;
    set<string> keys,stations,networks,loccodes,sta2set;
    size_t nlive(0);
    for(size_t i=i0;i<=iend;++i)
    {
      /* Skip any members marked dead but put a special entry in chans_this_group*/
      if(d[i].dead())
      {
        chans_this_group.push_back("DEADCHANNEL");
        continue;
      }
      net=d[i].get_string(SEISMICMD_net);
      sta=d[i].get_string(SEISMICMD_sta);
      chan=d[i].get_string(SEISMICMD_chan);
      sta2.assign(chan,0,2);
      loc=d[i].get_string(SEISMICMD_loc);
      chans_this_group.push_back(chan);
      networks.insert(net);
      stations.insert(sta);
      loccodes.insert(loc);
      sta2set.insert(sta2);
      keys.insert(chan);
      ++nlive;
    }
    /* We have to abort this algorithm immediately if there are duplicate
    net or sta values as it automatically means the assumptions of the
    algorithm are violated.  The "immediately" is not so obvious
    because we have to do a lot of housecleaning - there is a return
    at the end of this code block for the true condition*/
    if( (stations.size()!=1) || (networks.size()!=1)
         || (loccodes.size()!=1) || (sta2set.size()!=1) )
    {
      /* Note this code segment is repeated too many times in this function.
      I may be wise at some point to make it a file scope function like dogtag.*/
      for(size_t i=i0;i<=iend;++i)
      {
        bundle.push_back(dynamic_cast<const CoreTimeSeries&>(d[i]));
        if( ! (d[i].is_empty()) )
        {
          hvec.push_back(dynamic_cast<ProcessingHistory*>(&d_const[i]));
        }
      }
      d3c=dogtag(bundle);
      if(hvec.size()>0) d3c.add_many_inputs(hvec);
      d3c.kill();
      stringstream ss;
      ss << "Irregular grouping:  inconsistent seed name codes in group"<<endl;
      if(stations.size()>1)
      {
        ss << "List of inconsistent station names:  ";
        for(auto sptr=stations.begin();sptr!=stations.end();++sptr)
           ss << *sptr << " ";
        ss << endl;
      }
      if(networks.size()>1)
      {
        ss << "List of inconsistent network names:  ";
        for(auto sptr=networks.begin();sptr!=networks.end();++sptr)
           ss << *sptr << " ";
        ss << endl;
      }
      if(loccodes.size()>1)
      {
        ss << "List of inconsistent location names:  ";
        for(auto sptr=loccodes.begin();sptr!=loccodes.end();++sptr)
        {
          /* have to handle the common case of a loc code defined by an
          empty string - not null empty */
          if(sptr->length()==0)
            //ss << "BLANK_LOC_CODE"<<" ";
            ss << "\"  \" ";
          else
            ss << *sptr <<" ";
        }
        ss << endl;
      }
      if(sta2set.size()>1)
      {
        ss << "List of inconsistent channel codes (first 2 characters should be unique and they are not):  ";
        for(auto sptr=sta2set.begin();sptr!=sta2set.end();++sptr)
           ss << *sptr << " ";
        ss << endl;
      }
      d3c.elog.log_error("BundleSEEDGroup",ss.str(),ErrorSeverity::Invalid);
      return d3c;
    }
    size_t nkeys=keys.size();

    if(nlive<=3)
    {
      /* We can handle deficient groups here by this algorithm.
      After conditionals we kill output when size is less than 3.*/
      for(size_t i=i0;i<=iend;++i)
      {
        bundle.push_back(dynamic_cast<const CoreTimeSeries&>(d[i]));
        if( ! (d[i].is_empty()) )
        {
          hvec.push_back(dynamic_cast<ProcessingHistory*>(&d_const[i]));
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
        if(d[i].live())
        {
          chan=chans_this_group[ii];
          keys.insert(chan);
          xref.insert(pair<string,size_t>(chan,i));
        }
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
            bundle.push_back(dynamic_cast<const CoreTimeSeries&>
                    (d[xptr->second]));
            if( ! (d[xptr->second].is_empty()) )
            {
              hvec.push_back(dynamic_cast<ProcessingHistory*>
                      (&(d_const[xptr->second])));
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
            bundle.push_back(dynamic_cast<const CoreTimeSeries&>(d[jjmin]));
            if( ! (d[jjmin].is_empty()) )
            {
              hvec.push_back(dynamic_cast<ProcessingHistory*>(&d_const[jjmin]));
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
          bundle.push_back(dynamic_cast<const CoreTimeSeries&>(d[i]));
          if( ! (d[i].is_empty()) )
          {
            hvec.push_back(dynamic_cast<ProcessingHistory*>(&d_const[i]));
          }
        }
      }
    }
    if( (nkeys==3) && (nlive>=3) )
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
      d3c.elog.log_error("BundleSEEDGroup",ss.str(),ErrorSeverity::Invalid);
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
    /* this constructor clones the ensemble metadata */
    Ensemble<Seismogram> ens3c(dynamic_cast<Metadata&>(d),d.member.size()/3);
    vector<TimeSeries>::iterator dptr;
    string laststa,lastloc,lastchan,lastnet;
    string net(""),sta,chan,loc("");
    bool has_dead_channel(false);
    size_t i0,iend;
    size_t i;
    for(i=0,dptr=d.member.begin();dptr!=d.member.end();++i,++dptr)
    {
    //DEBUG
    cout<<"i="<<i<<" values: "
      << lastnet <<":"
      << laststa <<":"
      << lastchan <<":"
      << lastloc <<endl
      << "Current= "
      << net <<":"
      << sta <<":"
      << chan <<":"
      << loc <<endl;
      if(dptr->dead())
      {
        /* If net, sta, and loc are defined we try to blunder on so we can
        retain elog entries in data received as dead.  Necessary or the
        user won't be able to track the reason something was dropped easily*/
        if( dptr->is_defined(SEISMICMD_net) && dptr->is_defined(SEISMICMD_sta)
           && dptr->is_defined(SEISMICMD_loc) )
        {
          has_dead_channel=true;
        }
        else
        {
          /* In this situation we have to  just drop the bad datum and
          blunder on.  We assume the elog has no data that way and some
          other process handled this wrong .*/
          continue;
        }
      }
      if(i==0)
      {
        /* These things need to all be initialized by the first member*/
        if(dptr->is_defined(SEISMICMD_net))
        {
          lastnet=dptr->get<string>(SEISMICMD_net);
        }
        else
        {
          lastnet="Undefined";
        }
        if(dptr->is_defined(SEISMICMD_loc))
        {
          lastloc=dptr->get<string>(SEISMICMD_loc);
        }
        else
        {
          lastloc="Undefined";
        }
        laststa=dptr->get<string>(SEISMICMD_sta);
        lastchan=dptr->get<string>(SEISMICMD_chan);
        i0=0;
      }
      else
      {
        if(dptr->is_defined(SEISMICMD_net))
        {
          net=dptr->get<string>(SEISMICMD_net);
        }
        else
        {
          net="Undefined";
        }
        if(dptr->is_defined(SEISMICMD_loc))
        {
          loc=dptr->get<string>(SEISMICMD_loc);
        }
        else
        {
          loc="Undefined";
        }
        sta=dptr->get<string>(SEISMICMD_sta);
        chan=dptr->get<string>(SEISMICMD_chan);
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

          /* count the number of live channels when the has_dead_channel is set
          true.  We use that as a logic test to know if we should try to
          recover something */
          size_t nlive;
          if(has_dead_channel)
          {
            nlive=0;
            for(auto ii=i0;ii<=iend;++ii)
              if(d.member[ii].live())++nlive;
          }
          else
          {
            nlive=iend-i0+1;
          }
          //if((iend-i0)>=2)  //Use iend to handle end condition instead of i
          if(nlive>=3)
          {
            /* We can only have 3 live channels and iend-i0 be 2 if there are
            no channels marked dead */
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

              Among other things it has to handle channels marked dead
              */
              Seismogram dgrp(BundleSEEDGroup(d.member,i0,iend));
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
              <<"Number marked live="<<nlive<<endl
              <<"Number live must be at least 3"<<endl;
            d3c.elog.log_error("bundle_seed_data",ss.str(),ErrorSeverity::Invalid);
            ens3c.member.push_back(d3c);
          }
          laststa=sta;
          lastnet=net;
          lastloc=loc;
          lastchan=chan;
          i0=i;
          has_dead_channel=false;
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
