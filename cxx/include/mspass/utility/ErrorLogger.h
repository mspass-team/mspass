#ifndef _ERROR_LOGGER_H_
#define _ERROR_LOGGER_H_
#include <unistd.h>
#include <list>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/list.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include "mspass/utility/MsPASSError.h"
#include "mspass/utility/ErrorLogger.h"
namespace mspass
{
namespace utility{
class LogData
{
public:
  int job_id;
  int p_id;  // output of getpid()
  std::string algorithm;
  mspass::utility::ErrorSeverity badness;
  std::string message;
  LogData(){};
  /*! Normal constuctor from a MsPASSError or child of same.

  \param jid is the value to assign to job_id.
  \param alg is assigned to algorithm attribute.
  \param merr is parsed to fill the message and severity fields.
  Note p_id is always fetched with the system call getpid in the constructor.*/
  LogData(const int jid, const std::string alg,const mspass::utility::MsPASSError& merr);
  /*! Normal constuctor from strings.

  \param jid is the value to assign to job_id.
  \param alg is assigned to algorithm attribute.
  \param msg is the error message.
  \param lvl is the error severity.
  Note p_id is always fetched with the system call getpid in the constructor.*/
  LogData(const int jid, const std::string alg, const std::string msg, const mspass::utility::ErrorSeverity lvl);
  friend std::ostream& operator<<(std::ostream&, LogData&);
private:
  friend boost::serialization::access;
  template<class Archive>
     void serialize(Archive& ar,const unsigned int version)
  {
    ar & job_id;
    ar & p_id;
    ar & algorithm;
    ar & badness;
    ar & message;
  };
};
/*! \brief Container to hold error logs for a data object.

This class is intended mainly to be added to data objects in mspass to
provide a scalable, thread safe method for logging errors.  Atomic mspass data
objects (e.g. seismograms and time series objects) all use this class to
log errors and mark data with ambiguous states.   The log can explain why
data is an invalid state, but can also contain debug information normally
enabled by something like a verbose option to a program.  */
class ErrorLogger
{
public:
  ErrorLogger(){job_id=0;};
  ErrorLogger(int job)
  {
    job_id=job;
  };
  ErrorLogger(const ErrorLogger& parent);
  void set_job_id(int jid){job_id=jid;};
  int get_job_id(){return job_id;};
  /*! Logs one error message.

  \param merr - many mspass procedures throw MsPASSError objects.
    This simplifies the process of posting them to an error log.

  \return size of error log after insertion.
  */
  int log_error(const mspass::utility::MsPASSError& merr);
  /*! Log one a message directly with a specified severity.

    This is a convenience overload of log_error.  It splits the
    MsPASSError components as arguments with a default that
    allows a default behavior of Invalid as the error state.

    \param alg is name of algorithm posting this message
    \param mess is the message to be posted.
    \param level is the badness level to be set with the message.
       (default is ErrorSeverity::Invalid).

    \return size of error log after insertion.
    */
  int log_error(const std::string alg, const std::string mess,
		  const mspass::utility::ErrorSeverity level);

  /*! \brief Log a verbose message marking it informational.

  Frequently programs need a verbose option to log something of interest
  that is not an error but potentially useful.   This alternate logging
  method posts the string mess and marks it Informational. Returns
  the size of the log after insertion.
  */
  int log_verbose(const std::string alg, const std::string mess);
  std::list<LogData> get_error_log()const{return allmessages;};
  int size()const{return allmessages.size();};
  ErrorLogger& operator=(const ErrorLogger& parent);
  /*! Return an std::list container with most serious error level marked. */
  std::list<LogData> worst_errors()const;
private:
  int job_id;
  std::list<LogData> allmessages;
  friend boost::serialization::access;
  template<class Archive>
     void serialize(Archive& ar,const unsigned int version)
  {
    ar & job_id;
    ar & allmessages;
  };
};

/*! \brief Full test of error log for data validity.

This template can only work on mspass atomic data (TimeSeries and
Seismogram objects).  It assumes the data have an attribute elog that
is an ErrorLogger object containing the data's error log.
It returns immediately if the data are already marked dead.  Otherwise
it runs the worst_errors method on elog.   If the log is empty it returns
false as that implies no errors have been logged.   If there are entries
it looks for Invalid or Fatal entries and returns true only if there are
any entries of that level of severity.  i.e. it returns false if the only
errors are things like Complaint or less.
*/
template <typename Tdata> bool data_are_valid(const Tdata& d)
{
  if(d.dead()) return false;
  std::list<LogData> welog;
  welog=d.elog.worst_errors();
  /*The return will be empty if there are no errors logged*/
  if(welog.size()<=0) return true;
  LogData ld;
  /* Worst errors can return a list of multiple entries.  For
  fatal or invalid id should never be more than one, but this is
  a clean way to extract the first member of the list if it isn't empty*/
  ld=*(welog.begin());
  if(ld.badness == ErrorSeverity::Fatal || ld.badness == ErrorSeverity::Invalid)
      return false;
  else
      return true;
}
} // end utility namespace
} // End mspass namespace
#endif
