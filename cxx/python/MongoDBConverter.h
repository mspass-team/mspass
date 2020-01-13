#ifndef _MONGODB_CONVERTER_H_
#define _MONGODB_CONVERTER_H_
#include <string>
#include <pybind11/pybind11.h>
#include "mspass/utility/Metadata.h"
#include "mspass/utility/MetadataDefinitions.h"
using std::string;
using mspass::Metadata;
using mspass::MetadataDefinitions;
namespace mspass{
/*! \brief Interface routine to generate python structure for interacting with MongoDB.

The idea behind this class is provide a mechanism for a python script to
generate CRUD commands to drive MongoDB via pymongo.   The reasons this is
useful it avoids the need to build an interface to C++ for doing mongodb
transactions.

Initial focus is Metadata, but the intent is that this object could also
translate the std::vector used in TimeSeries and/or the dmatrix used in
Seismogram for serialization into the database.  That will be necessary only if
we find that approach is efficient enough to be an appropriate approach.
*/
namespace py=pybind11;
class MongoDBConverter
{
public:
  /*! Default constructor.  Loads default MetadataDefinitions. */
  MongoDBConverter() : mdef(mspass::MetadataDefinitions()){};
  /*! Construct from an already constructed MetadataDefinitions object. */
  MongoDBConverter(const mspass::MetadataDefinitions& mdefinition)
    : mdef(mdefinition){};
  /*! Construct using a name driven tag that defines a MetadataDefinitions object. */
  MongoDBConverter(const std::string mdefname);
  /*! \brief Return all Metadata marked as changed.

  In mspass the Metadata object keeps track of all updates and has methods
  to establish if an attribute has changed or not.  This method returns
  only those attributes marked as change.

  \param d is the data object to handle.  Normally a TimeSeries or Seismogram.
  \param verbose - when true any nonfatal errors are writen to stderr
    (use for debugging interactive python scripts)
  \return python dictionary that pymongo requires for mongodb CRUD operations
  \exception a MsPASError is thrown if data for a key marked in Metadata as changed
  is not present.  That should never happen so it is a major expection marked as fatal.
  */
  py::dict modified(const mspass::Metadata& d,bool verbose) const;
  /*! \brief Return all Metadata for MongoDB CRUD operations.

  This method returns all elements of a data object (normally a TimeSeries
  or Seismogram cast to Metadata) Metadata as a python dictionary (dict).
  Such a dictionary is the input pymongo uses for CRUD operations.
  \param d is the input data object
  \param verbose - when true any nonfatal errors are writen to stderr
    (use for debugging interactive python scripts)

  \exception A MsPASError object is thrown if the size of the dict created
  does not match the size of the original Metadata map.   That should
  never happen so the returned error is marked Fatal.
  */
  py::dict all(const mspass::Metadata& d,bool verbose) const;
  /*! \brief Return a python dict for CRUD operations driven by a list of keys.

  This method returns a python dict of Metadata defined by a list of keys.
  Types are sorted out internally.
  */
  py::dict selected(const mspass::Metadata& d,
    const py::list& keys,const bool noabort=true,bool verbose=false) const;
  /*! Method to find undefined keys.

  This method exists to handle the situation when a call to selected
  encounters undefined keys.   It returns a list of keys that are indefined.
  The idea is that this would be used by error handlers. We only define
  this for python lists assuming a C program would need this functionality*/
  py::list badkeys(const Metadata& d, const py::list& keys_to_test) const;

private:
  MetadataDefinitions mdef;
  py::dict extract_selected(const Metadata& d, const list<std::string>& keys,
     bool verbose) const;
};

} // End mspass namespace scope
#endif
