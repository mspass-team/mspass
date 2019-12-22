#ifndef _MSPASS_CORE_TS_H_
#define _MSPASS_CORE_TS_H_
#include "mspass/utility/ErrorLogger.h"
namespace mspass{
/*! \brief Common components for interaction with MsPASS.

This class is intended to be provide common API elements for data objects to interact with
MsPASS framework.   It contains elements needed by the data to interact seamlessly with MongoDB and
spark.  

This implementation assumes most database manipulations will take place in python scripts.  That
means the actual contents of this object are minimal.  For the initial implementation the main 
hook is the string that is used in MongoDB as an external form fo the ObjectID.   This assumption
is the get and put methods for the objectid string will be sufficient for a python wrapper to 
associate data with the a unique document in the database.   

A key feature this component contains is hooks to the ErrorLogger functionality of mspass.
That is, all data objects in mspass should not abort on most errors, but post messages to
the error log.   When objects are saved writers should normally drop data with errors posted
at some defined level (usually ErrorSeverity::Invalid).  Note the original design had the
error logging component as a nested/inner class of MsPASSCoreTS.   We found it was problematic
to handle tested classes with python wrappers so MsPASSCoreTS was made a child of ErrorLogger.
This strongly violates the normal rules for how the API should define this because 
ErrorLogger is a type example of a "has a" instead of an "is a".  
*/
class MsPASSCoreTS : public ErrorLogger
{
  public:
    /*! Default constructor.   Does almost nothing. */
    MsPASSCoreTS() : ErrorLogger(){hex_id="INVALID";};
    /*! Standard copy constructor. */
    MsPASSCoreTS(const MsPASSCoreTS& parent) : ErrorLogger(dynamic_cast<const ErrorLogger&>(parent))
    {
	hex_id=parent.hex_id;
    };
    /*! Standard assignment operator. */
    MsPASSCoreTS& operator=(const MsPASSCoreTS& parent)
    {
        if(this!=(&parent))
        {
            hex_id=parent.hex_id;
        }
        return *this;
    };
    void set_id(const string hexstring){hex_id=hexstring;};
    string get_id(){return hex_id;};
  private:
    /*! ObjectID hex string.   Can be converted readily to form needed to
    find a document. */
    string hex_id;
};
}  //End Namespace mspass
#endif
