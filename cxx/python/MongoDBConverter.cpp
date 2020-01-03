#include <stdint.h>
#include <list>
#include <set>
#include "mspass/utility/MsPASSError.h"
#include "MongoDBConverter.h"
namespace mspass{
using namespace mspass;
using namespace std;
/* This is a core private method that builds a dict from a MetadataList
object.  A MetadataList is sensible in C, but awkward for python.   Hence
we intentionally hide this behind the interface.   There is an inefficiency
here in returning the dict which will require a least two copy operations
when any method using this is called.  The assumption, however, is that
Metadata contents are not huge and this will not be a barrier. This assumption
could break down if the list of types were allowed to contain larger (in size)
objects.

An expected common issue is that the list will have keys that are not
defined in the input Metadata.  Rather than return a more complicated object
to handle this, all callers need to check the size of the output compared to
the input and handle that condition as appropriate*/
py::dict MongoDBConverter::extract_selected(const Metadata& md,
  const list<string>& keys) const noexcept
{
  py::dict result;
  if(keys.size()<=0) return result;
  list<string>::const_iterator kptr;
  for(kptr=keys.begin();kptr!=keys.end();++kptr)
  {
    double dval;
    float fval;
    int32_t i32val;
    long int ival;
    bool bval;
    string sval;
    try{
      string keystr(*kptr);
      /* pybind11 operator[] for a dict seems to only work with a
      C char* key.  Use of a std::string creates an errors so we use this
      pointer to make the logic clearer. */
      const char *key=keystr.c_str();
      MDtype mdt=mdef.type(key);
      switch(mdt)
      {
      case MDtype::Real:
      case MDtype::Real64:
      case MDtype::Double:
        dval=md.get<double>(key);
        result[key]=dval;
        break;
      case MDtype::Long:
      case MDtype::Int64:
      case MDtype::Integer:
        ival=md.get<long>(key);
        result[key]=ival;
        break;
      case MDtype::Int32:
        i32val=md.get<int32_t>(key);
        result[key]=i32val;
        break;
      case MDtype::Real32:
        fval=md.get<float>(key);
        result[key]=fval;
        break;
      case MDtype::String:
        sval=md.get<string>(key);
        result[key]=sval;
        break;
      case MDtype::Boolean:
        /* this one is tricky because of a mismatch with python's rather
        ridiculous concept of a boolean as a class */
        bval=md.get<bool>(key);
        if(bval)
        {
          result[key]=py::bool_(true);
        }
        else
        {
          result[key]=py::bool_(false);
        }
        break;
      case MDtype::Invalid:
      default:
        continue;
      // do nothing for default - should not happen but will lead size mismatch
      };
    }catch(...){continue;};  // do nothing if there are errors
  }
  return result;
}
MongoDBConverter::MongoDBConverter(const std::string mdefname)
{
try{
  mdef=MetadataDefinitions(mdefname);
}catch(...){throw;};
}
py::dict MongoDBConverter::modified(const mspass::Metadata& d) const
{
  try{
    py::dict result;
    set<string> tochange=d.modified();
    /* Nothing changing may be common so we return immediately around
    we return an empty dict in that case.*/
    if(tochange.size()<=0) return result;
    /* api mismatch.  Other methods are driven by a list so convenient to
    convert to a list for this method so a common function can be called. */
    list<string> clist;
    set<string>::iterator sptr;
    for(sptr=tochange.begin();sptr!=tochange.end();++sptr)
    {
      clist.push_back(*sptr);
    }
    result=this->extract_selected(d,clist);
    return result;
  }catch(...){throw;};
}
py::dict MongoDBConverter::all(const mspass::Metadata& d) const
{
  try{
    set<string> klist=d.keys();
    list<string> clist;
    set<string>::iterator sptr;
    for(sptr=klist.begin();sptr!=klist.end();++sptr)
    {
      clist.push_back(*sptr);
    }
    return(this->extract_selected(d,clist));
  }catch(...){throw;};
}
py::dict MongoDBConverter::selected(const mspass::Metadata& d,
  const py::list& keys, bool noabort) const
{
  try{
    if(keys.size()<=0) return(py::dict());
    /* We have to convert the python list to an std list */
    std::list<string> keystd;
    /* Using this C11 for loop, but may require a cast as not sure if
    python strings will be cast properly.   */
    for(auto a : keys)
    {
      /* I think this will work because a python string is a child
      of the base class object where the cast template is defined */
      string keystr=a.cast<string>();
      keystd.push_back(keystr);
    }
    /* we use the noabort argument to optionally abort if some attributes
    weren't copies. */
    py::dict result=this->extract_selected(d,keystd);
    if(noabort)
      return result;
    else
    {
      stringstream ss;
      ss << "MongoDBConverter::selected method: copy error"<<endl
        << "Input list of keys length="<<keys.size()<<endl
        << "Output python dict size="<<result.size()<<endl
        << "Assuming this is an interactive script"<<endl
        << "Run keys_to_test method, fix the list, and try again"
        <<endl;
      throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
    }
  }catch(...){throw;};
}
py::list MongoDBConverter::badkeys(const Metadata& d,
  const py::list& keys_to_test) const
{
  try{
    py::list badlist;
    for(auto a : keys_to_test)
    {
      string keystr=a.cast<std::string>();
      /* Note inverted logic from name - execute when key is not defined */
      if(!d.is_defined(keystr))
      {
        badlist.append(a);
      }
    }
    return badlist;
  }catch(...){throw;};
}
}  // End mspass namespace encapsulation
