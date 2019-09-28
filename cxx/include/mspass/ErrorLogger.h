#ifndef _ERROR_LOGGER_H_
#define _ERROR_LOGGER_H_
#include <unistd.h>
#include <list>
#include <list>
#include "MsPASSError.h"
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

  \return size of error log after insertion.
  */
  int log_error(mspass::MsPASSError& merr);
  /*! \brief Log a verbose message marking it informational.

  Frequently programs need a verbose option to log something of interest
  that is not an error but potentially useful.   This alternate logging
  method posts the string mess and marks it Informational. Returns
  the size of the log after insertion.
  */
  int log_verbose(std::string mess);
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
