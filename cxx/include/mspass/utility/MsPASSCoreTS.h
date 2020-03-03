#ifndef _MSPASS_CORE_TS_H_
#define _MSPASS_CORE_TS_H_
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
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
*/
class MsPASSCoreTS
{
  public:
    /*! \brief  Error logging component.

    Data processing always involves grey areas of errors.   Some errors invalidate the data and
    others make it just questionable.   Some errors should be fatal, but we may not want to
    abort everything in a large job running for many hours.   This object in MsPASS implements
    a generic, scalable log mechanism where objects can post errors to the log and abort only
    when essential.   The idea is that log messages are cached with the data and saved only
    when we ask the data in a container to be saved.   Intermediate processing results can also
    be tested for validity through the ErrorLogger API.  */
    ErrorLogger elog;
    /*! Default constructor.   Does almost nothing. */
    MsPASSCoreTS() : elog(){hex_id="INVALID";};
    /*! Standard copy constructor. */
    MsPASSCoreTS(const MsPASSCoreTS& parent){hex_id=parent.hex_id;};
    /*! Standard assignment operator. */
    MsPASSCoreTS& operator=(const MsPASSCoreTS& parent)
    {
        if(this!=(&parent))
        {
            hex_id=parent.hex_id;
            elog=parent.elog;
        }
        return *this;
    };
    void set_id(const string hexstring){hex_id=hexstring;};
    string get_id(){return hex_id;};
  private:
    /*! ObjectID hex string.   Can be converted readily to form needed to
    find a document. */
    string hex_id;
    friend boost::serialization::access;
    template<class Archive>
       void serialize(Archive& ar,const unsigned int version)
    {
      ar & hex_id;
      ar & elog;
    };
};
}  //End Namespace mspass
#endif
