#ifndef _MSPASS_ERROR_H_
#define _MSPASS_ERROR_H_
#include <iostream>
#include <exception>
namespace mspass {
using namespace std;
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
/*!
 Default constructor built inline.
**/
	MsPASSError(){message="MsPASS library error\n";};
/*! Construct from a std::string
**/
	MsPASSError(const string mess){message=mess;};
/*! Construct from a char *
**/
	MsPASSError(const char *mess){message=string(mess);};
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
  }
};
}  // End mspass namespace declaration
#endif
