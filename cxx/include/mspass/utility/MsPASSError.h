#ifndef _MSPASS_ERROR_H_
#define _MSPASS_ERROR_H_
#include <iostream>
#include <exception>
namespace mspass {
using namespace std;
/*! \brief Severity code for error messages.

C++11 added the construct of a enum class that we use here.  It makes
the enum more strongly typed to avoid collisions for these common 
words that would be trouble otherwise.   This class is used in
all MsPASSError objects. */
enum class ErrorSeverity
{
	Fatal,
	Invalid,
	Suspect,
	Complaint,
	Debug,
	Informational
};
/*! \brief Base class for error object thrown by MsPASS library routines.

 This is the generic error object thrown by the MsPASS library.
 it is similar in concept to basic error objects described in various
 books by Stroustrup.  The base object contains only a simple
 generic message and a virtual log_error method common to all
 MsPASS error objects that are it's descendents.
\author Gary L. Pavlis
**/
class MsPASSError : public std::exception
{
public:
/*!
 Holds error message that can be printed with log_error method.
**/
	string message;
	ErrorSeverity badness;
/*!
 Default constructor built inline.
**/
	MsPASSError(){
		message="MsPASS library error\n";
		badness=ErrorSeverity::Fatal;
	};
/*! \brief Construct from a std::string with badness defined by keywords in a string.

Sometimes it is easier and more readable to use a string literal to define
the error class.  This uses that approach.

\param mess is the error message posted.
\param howbad is a string to translate to one of the allowed enum values.  The
  allowed values are the same as the enum defined in this file:
	FATAL,Invalid,Suspect,Complaint,Debug,Informational.
**/
	MsPASSError(const string mess,const char *howbad){
		message=mess;
		string s(howbad);
		if(s=="Fatal"){
		  badness=ErrorSeverity::Fatal;
		}
		else if (s=="Invalid"){
			badness=ErrorSeverity::Invalid;
		}
		else if (s=="Suspect"){
			badness=ErrorSeverity::Suspect;
		}
		else if (s=="Complaint"){
			badness=ErrorSeverity::Complaint;
		}
		else if (s=="Debug"){
			badness=ErrorSeverity::Debug;
		}
		else if (s=="Informational"){
			badness=ErrorSeverity::Informational;
		}
		else
		{
			/* Default to Fatal in this case as this should only happen in
			a debug situation. */
			badness=ErrorSeverity::Fatal;
		}
	};
/*! Construct from a string with enum defining severity.

This should be the normal form of this error object to throw.   Default of
the enum allows simpler usage for most errors.

\param mess - is the error message to be posted.
\param s is the severity enum (default Invalid).
*/
	MsPASSError(const string mess,const ErrorSeverity s=ErrorSeverity::Invalid)
        {
            message=mess;
            badness=s;
        };
/*! Construct from a char * and severity enum.
**/
	MsPASSError(const char *mess,const ErrorSeverity s){
		message=string(mess);
		badness=s;
	};
/*!
 Sends error message thrown by MsPASS library functions to standard error.
**/
void log_error(){
  cerr << message << endl;
};
/*! Overloaded method for sending error message to other than stderr. */
void log_error(ostream& ofs)
{
	ofs << message <<endl;
}
/*! This overrides the method in std::exception to load our string.
  This allows handlers to use the what method and get the error string
  from seisp.  Idea copied from:
  http://www.cplusplus.com/doc/tutorial/exceptions/
  */
  const char * what() const throw()
  {
    return message.c_str();
  };
	/*! Return error severity as the enum value. */
	ErrorSeverity severity(){return badness;};
};
}  // End mspass namespace declaration
#endif
