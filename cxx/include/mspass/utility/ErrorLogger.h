#ifndef _ERROR_LOGGER_H_
#define _ERROR_LOGGER_H_
#include <unistd.h>
#include <list>
#include <list>
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
  LogData(int jid, std::string alg, mspass::MsPASSError& merr);
  friend ostream& operator<<(ostream&, LogData&);
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
  ErrorLogger(){};
  ErrorLogger(int job,std::string alg)
  {
    job_id=job;
    algorithm=alg;
  };
  ErrorLogger(const ErrorLogger& parent);
  void set_job_id(int jid){job_id=jid;};
  void set_algorithm(std::string alg){algorithm=alg;};
  int get_job_id(){return job_id;};
  std::string get_algorithm(){return algorithm;};
  /*! Logs one error message.

  \param merr - many mspass procedures throw MsPASSError objects.
    This simplifies the process of posting them to an error log.

  \return size of error log after insertion.
  */
  int log_error(mspass::MsPASSError& merr);
  /*! Logs an error with a specified badness level.
    \param mess - error message posted
    \param es - severity of error as defined by enum class ErrorSeverity

  \return size of error log after insertion.
  */
  int log_error(const std::string mess, const ErrorSeverity es);
  /*! \brief Log a verbose message marking it informational.

  Frequently programs need a verbose option to log something of interest
  that is not an error but potentially useful.   This alternate logging
  method posts the string mess and marks it Informational. Returns
  the size of the log after insertion.
  */
  int log_verbose(std::string mess)
  {
      int count;
      count=this->log_error(mess,ErrorSeverity::Informational);
      return count;
  };
  std::list<LogData> get_error_log(){return allmessages;};
  int size(){return allmessages.size();};
  ErrorLogger& operator=(const ErrorLogger& parent);
  /*! Return an std::list container with most serious error level marked. */
  std::list<LogData> worst_errors();
private:
  std::string algorithm;
  int job_id;
  std::list<LogData> allmessages;
};
} // End mspass namespace
#endif
