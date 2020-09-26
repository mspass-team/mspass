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


/*! Internal used to convert the keyword in a message string to an ErrorSeverity.*/
mspass::ErrorSeverity string2severity(const string howbad);
/*! Inverse of string2severity*/
string severity2string(const mspass::ErrorSeverity es);

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
 Default constructor built inline.
**/
	MsPASSError(){
		message="MsPASS library error\n";
		badness=ErrorSeverity::Fatal;
		full_message.assign(message + ":" + severity2string(badness));
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
		/* this small helper function parses s to conver to the
		enum class of badness*/
		badness=string2severity(s);
		full_message.assign(message + ":" + severity2string(badness));
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
			full_message.assign(message + ":" + severity2string(badness));
        };
/*! Construct from a char * and severity enum.
**/
	MsPASSError(const char *mess,const ErrorSeverity s){
		message=string(mess);
		badness=s;
		full_message.assign(message + ":" + severity2string(badness));
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

	pybind11 limitations make it problematic for
	python error handlers to get the output of the severity method or
	even get to the badness attribute.   See the cc code for the implementation
	detail.
  */
  const char * what() const noexcept{return full_message.c_str();};
	/*! Return error severity as the enum value. */
	ErrorSeverity severity() const {return badness;};
	/*! Return only the raw message string. */
	string core_message() const {return message;};
protected:
	/*!
	 Holds error message that can be printed with log_error method.
	**/
		string message;
		/*! Defines the severity of this error - see enum class above */
		ErrorSeverity badness;
		/*! Holds the full error message that includes badness. This is returned by the what method */
		string full_message;
};
/* Helper function prototypes.  The following set of functions proved necessary
because of a limitation in pybind11.  It appears there is no easy way to bind
a full interface to MsPASSError when it is handled as an exception.   As a result
neither the badness attribute (if made public) nor the severity method are
visible when treated as an exception in python.   To circumvent this the what
method writes a string representation of badness to the message.   Bound versions
of the functions below can be used in python programs to test severity of
a thrown MsPASSError.
*/
/*! \brief Test if a thrown error indicates data are no longer valid.

Errors vary in severity.  Use this function as fast test for an error so
severe an algorithm failed and any subsequent use of the data is ill advised.
*/
bool error_says_data_bad(const mspass::MsPASSError& err);

/*! \brief Return a string representation of error severity.

This function uses the what method to retrieve the message string with the
error severity appended.  It then simply returns the keyword defining the
error code contained in the message.   The list is:  Fatal,Invalid,suspect,
Complaint,Debug, or Informational.   It will return Fatal if there is no
match to any of the keywords assuming something has corrupted memory for
that to happen.
*/
string parse_message_error_severity(const mspass::MsPASSError& err);
/*! \brief return message severity as an ErrorSeverity enum class.

ErrorSeverity is bound with pybind11 and provides a way to define the
severity of an error.  This function is a close companion to parse_message_error_severity.
In fact, in the current implementation it calls parse_message_error_severity and converts
the result string to an ErrorSeverity it returns.    That process has a small
overhead and it is largely a decision of asthetics whether or not to use this
function of parse_message_error_severity.
*/
mspass::ErrorSeverity message_error_severity(const mspass::MsPASSError& err);
}  // End mspass namespace declaration
#endif
