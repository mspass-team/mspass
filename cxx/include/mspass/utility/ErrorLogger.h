#ifndef _ERROR_LOGGER_H_
#define _ERROR_LOGGER_H_
#include <unistd.h>
#include <list>
#include <list>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/list.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include "mspass/utility/MsPASSError.h"
#include "mspass/utility/ErrorLogger.h"
namespace mspass
{
class LogData
{
public:
  int job_id;
  int p_id;  // output of getpid()
  std::string algorithm;
  mspass::ErrorSeverity badness;
  std::string message;
  LogData(){};
  /*! Normal constuctor from a MsPASSError or child of same.

  \param jid is the value to assign to job_id.
  \param alg is assigned to algorithm attribute.
  \param merr is parsed to fill the message and severity fields.
  Note p_id is always fetched with the system call getpid in the constructor.*/
  LogData(const int jid, const std::string alg,const mspass::MsPASSError& merr);
  friend ostream& operator<<(ostream&, LogData&);
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
  int log_error(const mspass::MsPASSError& merr);
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
		  const mspass::ErrorSeverity level);

  /*! \brief Log a verbose message marking it informational.

  Frequently programs need a verbose option to log something of interest
  that is not an error but potentially useful.   This alternate logging
  method posts the string mess and marks it Informational. Returns
  the size of the log after insertion.
  */
  int log_verbose(const std::string alg, const std::string mess);
  std::list<LogData> get_error_log(){return allmessages;};
  int size(){return allmessages.size();};
  ErrorLogger& operator=(const ErrorLogger& parent);
  /*! Return an std::list container with most serious error level marked. */
  std::list<LogData> worst_errors();
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
} // End mspass namespace
#endif
