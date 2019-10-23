#ifndef _MSPASS_CORE_TS_H_
#define _MSPASS_CORE_TS_H_
#include "mspass/utility/ErrorLogger.h"
namespace mspass{
/*! \brief Common components for interaction with MsPASS.

This class is intended to be provide common API elements for data objects to interact with
MsPASS framework.   It contains elements needed by the data to interact seamlessly with MongoDB and
spark.   A convention we'l use is that all data objects that inherit this class will have MP as
the first two characters in the name.  e.g. MPSeismogram is Seismogram with this class as a child.
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
    MsPASSCoreTS() : elog(){};
    /*! Standard copy constructor. */
    MsPASSCoreTS(const MsPASSCoreTS& parent);
    /*! Standard assignment operator. */
    MsPASSCoreTS& operator=(const MsPASSCoreTS& parent);
    //Objectid get_id();
    //void set_id(const ObjectId& newid);
    void set_id(const string hexstring);
  private:
    /*! ObjectID hex string.   Can be converted readily to form needed to
    find a document. */
    string hex_id;
};
}  //End Namespace mspass
#endif
