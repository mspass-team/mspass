#include "mspass/utility/AttributeCrossReference.h"
#include <sstream>
using namespace std;
using namespace mspass::utility;
namespace mspass::utility {
AttributeCrossReference::AttributeCrossReference(const string lines_to_parse) {
  try {
    istringstream instrm(lines_to_parse);
    do {
      string inkey, outkey;
      string typestr;
      instrm >> inkey;
      // unfortunately this is the normal exit
      if (instrm.eof())
        break;
      instrm >> outkey;
      instrm >> typestr;
      itoe.insert(pair<string, string>(inkey, outkey));
      etoi.insert(pair<string, string>(outkey, inkey));
      if (typestr == "int" || typestr == "INT" || typestr == "integer")
        imdtypemap.insert(pair<string, MDtype>(inkey, MDtype::Integer));
      else if (typestr == "real" || typestr == "REAL" || typestr == "double")
        imdtypemap.insert(pair<string, MDtype>(inkey, MDtype::Double));
      else if (typestr == "bool" || typestr == "BOOL" || typestr == "boolean")
        imdtypemap.insert(pair<string, MDtype>(inkey, MDtype::Boolean));
      else if (typestr == "string" || typestr == "STRING")
        imdtypemap.insert(pair<string, MDtype>(inkey, MDtype::String));
      else {
        /* Note in seispp this condition only logs and error and
         * sets attribute invalid.  Here we throw an exception as
         * this would always be called on startup and should normally
         * cause an abort */
        stringstream sserr;
        sserr << "AttributeCrossReference string constructor:  "
              << " Attribute with tag=" << inkey << " is tagged with an "
              << "illegal type name=" << typestr << endl
              << "Repair input data passed to this constructor" << endl;
        throw MsPASSError(sserr.str());
      }

    } while (!instrm.eof());
  } catch (...) {
    throw;
  };
}
/*! This constructor was produced by a revisio of the previous to change
 * from a single string to a list container.   */
AttributeCrossReference::AttributeCrossReference(const list<string> &lines) {
  list<string>::const_iterator lptr;
  for (lptr = lines.begin(); lptr != lines.end(); ++lptr) {
    istringstream instrm(*lptr);
    string inkey, outkey;
    string typestr;
    instrm >> inkey;
    instrm >> outkey;
    instrm >> typestr;
    itoe.insert(pair<string, string>(inkey, outkey));
    etoi.insert(pair<string, string>(outkey, inkey));
    if (typestr == "int" || typestr == "INT" || typestr == "integer")
      imdtypemap.insert(pair<string, MDtype>(inkey, MDtype::Integer));
    else if (typestr == "real" || typestr == "REAL" || typestr == "double")
      imdtypemap.insert(pair<string, MDtype>(inkey, MDtype::Double));
    else if (typestr == "bool" || typestr == "BOOL" || typestr == "boolean")
      imdtypemap.insert(pair<string, MDtype>(inkey, MDtype::Boolean));
    else if (typestr == "string" || typestr == "STRING")
      imdtypemap.insert(pair<string, MDtype>(inkey, MDtype::String));
    else {
      stringstream sserr;
      sserr << "AttributeCrossReference string constructor:  "
            << " Attribute with tag=" << inkey << " is tagged with an "
            << "illegal type name=" << typestr << endl
            << "Repair input data passed to this constructor" << endl;
      throw MsPASSError(sserr.str());
    }
  }
}
AttributeCrossReference::AttributeCrossReference(
    const map<string, string> int2ext, const MetadataList &mdlist) {
  MetadataList::const_iterator mdlptr;
  for (mdlptr = mdlist.begin(); mdlptr != mdlist.end(); ++mdlptr) {
    imdtypemap.insert(pair<string, MDtype>(mdlptr->tag, mdlptr->mdt));
  }
  itoe = int2ext;
  map<string, string>::iterator iptr;
  /* This extracts each pair of the map and inverts them */
  for (iptr = itoe.begin(); iptr != itoe.end(); ++iptr)
    etoi.insert(pair<string, string>(iptr->second, iptr->first));
}
AttributeCrossReference::AttributeCrossReference(
    const AttributeCrossReference &parent) {
  itoe = parent.itoe;
  etoi = parent.etoi;
  imdtypemap = parent.imdtypemap;
}
AttributeCrossReference &
AttributeCrossReference::operator=(const AttributeCrossReference &parent) {
  if (this != &parent) {
    itoe = parent.itoe;
    etoi = parent.etoi;
    imdtypemap = parent.imdtypemap;
  }
  return (*this);
}
string AttributeCrossReference::internal(const string key) const {
  map<string, string>::const_iterator iptr;
  iptr = etoi.find(key);
  if (iptr == etoi.end())
    throw MsPASSError(string("AttribureCrossReference::internal:  ") +
                      "Cannot find attribute " + key +
                      " in external to internal namespace map");
  return (iptr->second);
}
string AttributeCrossReference::external(const string key) const {
  map<string, string>::const_iterator iptr;
  iptr = itoe.find(key);
  if (iptr == itoe.end())
    throw MsPASSError(string("AttribureCrossReference::external:  ") +
                      "Cannot find attribute " + key +
                      " in internal to external  namespace map");
  return (iptr->second);
}
MDtype AttributeCrossReference::type(const string key) const {
  map<string, MDtype>::const_iterator iptr;
  iptr = imdtypemap.find(key);
  if (iptr == imdtypemap.end())
    throw MsPASSError(string("AttributeCrossReference::type:  ") +
                      "Cannot find attribute " + key + " in type definitions");
  return (iptr->second);
}
int AttributeCrossReference::size() const {
  // Assume the two maps are the same size
  return (itoe.size());
}
void AttributeCrossReference::put(const string i, const string e) {
  itoe.insert(pair<string, string>(i, e));
  etoi.insert(pair<string, string>(e, i));
}
/* These two methods could use either the etoi or the itoe containers
   to fetch the appropriate keys, but we always use fetch the
   first of the pair as that will always yield a unique set that
   exactly matches the originating map. */
set<string> AttributeCrossReference::internal_names() const {
  map<string, string>::const_iterator mptr;
  set<string> keys;
  for (mptr = itoe.begin(); mptr != itoe.end(); ++mptr) {
    keys.insert(mptr->first);
  }
  return keys;
}
set<string> AttributeCrossReference::external_names() const {
  map<string, string>::const_iterator mptr;
  set<string> keys;
  for (mptr = etoi.begin(); mptr != etoi.end(); ++mptr) {
    keys.insert(mptr->first);
  }
  return keys;
}

} // namespace mspass::utility
