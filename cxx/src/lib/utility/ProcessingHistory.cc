#include "mspass/utility/MsPASSError.h"
#include "mspass/utility/ProcessingHistory.h"

using namespace std;
using namespace mspass;
namespace mspass{
/* This is an internal function that returns a string description of the
ProcessingStatus enum class */
string status_to_words(const ProcessingStatus status)
{
  string word;
  switch(status)
  {
    case ProcessingStatus::RAW:
      word=string("RAW");
      break;
    case ProcessingStatus::ORIGIN:
      word=string("ORIGIN");
      break;
    case ProcessingStatus::VOLATILE:
      word=string("VOLATILE");
      break;
    case ProcessingStatus::SAVED:
      word=string("SAVED");
      break;
    default:
      word=string("UNDEFINED");
  };
  return word;
}
/* ProcessingHistoryRecord is really a struct that holds data
stored in ProcessingHistory, but we define these
constructors to assure the struct is  handled right in std containers.
i.e. not sure it is safe to use compiler generated defaults*/
ProcessingHistoryRecord::ProcessingHistoryRecord() :
  algorithm(),instance(),id(),inputs()
{
  status=ProcessingStatus::UNDEFINED;
}
ProcessingHistoryRecord::ProcessingHistoryRecord (const ProcessingHistoryRecord& parent)
  : algorithm(parent.algorithm),
       instance(parent.instance),
         id(parent.id),
           inputs(parent.inputs)
{
  status=parent.status;
}
ProcessingHistoryRecord& ProcessingHistoryRecord::operator=
  (const ProcessingHistoryRecord& parent)
{
  if(this!=(&parent))
  {
    algorithm=parent.algorithm;
    instance=parent.instance;
    id=parent.id;
    inputs=parent.inputs;
    status=parent.status;
  }
  return *this;
}
/* Start of ProcessingHistory code. */
/* PLACEHOLDER - fix inheritance issue for pybind11.   Temporary change
to get pybind11 wrappers to compile
ProcessingHistory::ProcessingHistory() : BasicProcessingHistory(),elog()
*/
ProcessingHistory::ProcessingHistory()
{
  status=ProcessingStatus::UNDEFINED;
}
ProcessingHistory::ProcessingHistory(const ProcessingHistory& parent)
  : BasicProcessingHistory(parent),elog(parent.elog),
      history_list(parent.history_list)
{
  status=parent.status;
}
bool ProcessingHistory::is_raw()
{
  if(status==ProcessingStatus::RAW)
    return true;
  else
    return false;
}
bool ProcessingHistory::is_origin()
{
  if(status==ProcessingStatus::RAW || status==ProcessingStatus::ORIGIN)
    return true;
  else
    return false;
}
bool ProcessingHistory::is_volatile()
{
  if(status==ProcessingStatus::VOLATILE)
    return true;
  else
    return false;
}
bool ProcessingHistory::is_saved()
{
  if(status==ProcessingStatus::SAVED)
    return true;
  else
    return false;
}
size_t ProcessingHistory::current_stage()
{
  return history_list.size();
}

size_t ProcessingHistory::new_stage(const ProcessingHistoryRecord& rec)
{
  shared_ptr<ProcessingHistoryRecord> ptr(new ProcessingHistoryRecord(rec));
  history_list.push_back(ptr);
  status=rec.status;
  return history_list.size();
};
/* Note we don't distinguish raw and origin here - rec must define it one
way or the other. */
void ProcessingHistory::set_as_origin(const ProcessingHistoryRecord& rec)
{
  const string base_error("ProcessingHistory::set_as_origin:  ");
  switch(rec.status)
  {
    case ProcessingStatus::RAW:
    case ProcessingStatus::ORIGIN:
      break;
    default:
      throw MsPASSError(base_error
        + "ProcessingHistoryRecord pass does not have status marked as RAW or ORIGIN",
        ErrorSeverity::Invalid);
  };
  if(history_list.size()==0)
  {
    this->new_stage(rec);
  }
  else
  {
    throw MsPASSError(base_error
      +"Illegal usage.  History chain must be empty to call this method.  Use clear() first.",
        ErrorSeverity::Invalid);
  }
}
size_t ProcessingHistory::set_as_saved(const ProcessingHistoryRecord& rec)
{
  const string base_error("ProcessingHistory::set_as_saved:  ");
  switch(rec.status)
  {
    case ProcessingStatus::SAVED:
      break;
    default:
      throw MsPASSError(base_error
        + "ProcessingHistoryRecord pass does not have status marked as SAVED",
        ErrorSeverity::Invalid);
  };
  this->new_stage(rec);
  /* This is a fragile way to test if the id is an objectid.  It is valid in
  2020 version of mongodb but if they ever change the definition of an object id
  this is trouble.  It doesn't actually throw an exception bust posts a
  warning to the elog.   */
  if(rec.id.size() != 12)
  {
    MsPASSError mpe(base_error
      + "id field does not appear to be a mongodb objectid string",
       ErrorSeverity::Complaint);
    elog.log_error(mpe);
  }
  return history_list.size();
}
void ProcessingHistory::reset(const ProcessingHistoryRecord& rec)
{
  history_list.clear();
  try{
    /* We use this method to enforce the origin rule for a reset. */
    this->set_as_origin(rec);
  }catch(...){throw;};
}

ProcessingHistory& ProcessingHistory::operator=(const ProcessingHistory& parent)
{
  if(this!=(&parent))
  {
    this->BasicProcessingHistory::operator=(parent);
    status=parent.status;
    history_list=parent.history_list;
  }
  return *this;
}
}//End mspass namespace encapsulation
