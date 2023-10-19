#include "mspass/utility/ErrorLogger.h"
using namespace mspass::utility;
namespace mspass::utility
{
using namespace std;

/* First code for the LogData class that are too long for inline */
LogData::LogData(const int jid, const std::string alg,
  const mspass::utility::MsPASSError& merr)
{
  job_id=jid;
  p_id=getpid();
  algorithm=alg;
  message=merr.what();
  badness=merr.severity();
}
LogData::LogData(const int jid, const std::string alg, const std::string msg, const mspass::utility::ErrorSeverity lvl)
{
  job_id=jid;
  p_id=getpid();
  algorithm=alg;
  message=msg;
  badness=lvl;
}
ostream& operator<<(ostream& ofs, LogData& ld)
{
  switch(ld.badness)
  {
    case ErrorSeverity::Fatal:
      ofs<<"Fatal ";
      break;
    case ErrorSeverity::Invalid:
      ofs<<"Invalid ";
      break;
    case ErrorSeverity::Complaint:
      ofs<<"Complaint ";
      break;
    case ErrorSeverity::Debug:
      ofs<<"Debug ";
      break;
    case ErrorSeverity::Informational:
      ofs<<"Informational ";
      break;
    default:
      ofs<<"Undefined"<<endl;
  }
  ofs<<ld.job_id<<" "<<ld.p_id<<" "<<ld.algorithm
      <<" "<<ld.message<<endl;
  return ofs;
}
/* Now the code for the ErrorLogger class */
ErrorLogger::ErrorLogger(const ErrorLogger& parent)
{
  job_id=parent.job_id;
  /* Just copy this.  Could call getpid every time, but the only way I can
  conceive that would be an issue is if the entry were serialized and sent
  somewhere through something like mpi.   A copy is a copy so seems best to
  clone this not ask for confusion. */
  allmessages=parent.allmessages;
}
ErrorLogger& ErrorLogger::operator=(const ErrorLogger& parent)
{
  if(this!=&parent)
  {
    job_id=parent.job_id;
    allmessages=parent.allmessages;
  }
  return *this;
}
ErrorLogger& ErrorLogger::operator+=(const ErrorLogger& other)
{
  if(this!=&other)
  {
    for(auto lptr=other.allmessages.begin();lptr!=other.allmessages.end();++lptr)
    {
      this->allmessages.push_back(*lptr);
    }
  }
  return *this;
}
int ErrorLogger::log_error(const mspass::utility::MsPASSError& merr)
{
  LogData thislog(this->job_id,string("MsPASSError"),merr);
  allmessages.push_back(thislog);
  return allmessages.size();
}
int ErrorLogger::log_error(const std::string alg, const std::string mess,
  const mspass::utility::ErrorSeverity level=ErrorSeverity::Invalid)
{
  LogData thislog(this->job_id,alg,mess,level);
  allmessages.push_back(thislog);
  return allmessages.size();
}
int ErrorLogger::log_verbose(const std::string  alg,const std::string mess)
{
  int count;
  count=this->log_error(alg,mess,ErrorSeverity::Informational);
  return count;
};
/* This method needs to return a list of the highest ranking
errors in the log.   This is a bit tricky because we specify badness with
an enum.   It seems dangerous to depend upon the old (I think depricated)
equivalence of an enum with an ordered list of ints.   Hence, we have a
bit of an ugly algorithm here.  The algorithm is a memory hog and would be
a bad idea if the log got huge, but for the indended use in mspass that
should never happen.  User beware if transported though - glp 9/28/2019.*/
list<LogData> ErrorLogger::worst_errors() const
{
  /* Return immediately if the messages container is empty. */
  if(allmessages.size()<=0) return allmessages;
  list<LogData> flist,ivlist,slist,clist,dlist,ilist;
  list<LogData>::const_iterator aptr;
  for(aptr=allmessages.begin();aptr!=allmessages.end();++aptr)
  {
    switch(aptr->badness)
    {
      case ErrorSeverity::Fatal:
        flist.push_back(*aptr);
        break;
      case ErrorSeverity::Invalid:
        ivlist.push_back(*aptr);
        break;
      case ErrorSeverity::Suspect:
        slist.push_back(*aptr);
        break;
      case ErrorSeverity::Complaint:
        clist.push_back(*aptr);
        break;
      case ErrorSeverity::Debug:
        dlist.push_back(*aptr);
        break;
      case ErrorSeverity::Informational:
      default:
        ilist.push_back(*aptr);
    };
  }
  if(flist.size()>0) return flist;
  if(ivlist.size()>0) return ivlist;
  if(slist.size()>0) return slist;
  if(clist.size()>0) return clist;
  if(dlist.size()>0) return dlist;
  return ilist;
}
}
