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
  const list<string>& keys,bool verbose) const
{
  py::dict result;
  if(keys.size()<=0) return result;
  list<string>::const_iterator kptr;
  for(kptr=keys.begin();kptr!=keys.end();++kptr)
  {
    double dval;
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
        dval=md.get_double(key);
        result[key]=dval;
        break;
      case MDtype::Long:
      case MDtype::Int64:
      case MDtype::Integer:
	/* Always use long for this as the intrinsic in python*/
        ival=md.get_long(key);
        result[key]=ival;
        break;
      case MDtype::Int32:
	/* We found for python interaction 32 bit ints and floats
	 * created havoc.   We thus always promote them to 64 bit versions.
	 * Writes to cerr in verbose mode are to help debugging problems.
	 * This depends on get_long autoconversion of this type - maintenance 
	 * issue if that ever changed.*/
	if(verbose) cerr << "MongoDBConverter:  data for key="<<key
		<< " defined int32 - promoting to int64"<<endl;
	ival=md.get_long(key);
        result[key]=ival;
        break;
      case MDtype::Real32:
	if(verbose) cerr << "MongoDBConverter:  data for key="<<key
		<< " defined float - promoting to double"<<endl;
	/* This also depends on auto conversion in get_double which is
	 * implementation dependent*/
	dval=md.get_double(key);
        result[key]=dval;
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
        if(verbose) cerr << "MongoDBConverter:  data for key="<<key
		<< " is marked Invalid and will be skipped"<<endl;
	break;
      default:
	if(verbose) cerr << "MongoDBConverter:  data for key="<<key
		<< " is marked with unknown type"<<endl
		<< "This should not happen and is a bug that should be reported"
		<<endl;
      };
    }catch(MsPASSError& merr){
      if(verbose)
        merr.log_error();
      else
        throw merr;
    }catch(...)
    {
      throw MsPASSError("MongoDBConverter::extract_selected:  Something threw an unexpected exception");
    };  // do nothing if there are errors
  }
  return result;
}
MongoDBConverter::MongoDBConverter(const std::string mdefname)
{
try{
  mdef=MetadataDefinitions(mdefname);
}catch(...){throw;};
}
py::dict MongoDBConverter::modified(const mspass::Metadata& d,bool verbose) const
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
      if(mdef.writeable(*sptr)) clist.push_back(*sptr);
    }
    result=this->extract_selected(d,clist,verbose);
    return result;
  }catch(...){throw;};
}
py::dict MongoDBConverter::writeable(const mspass::Metadata& d, bool verbose) const
{
  try{
    py::dict result;
    set<string> klist=d.keys();
    list<string> clist;
    set<string>::iterator sptr;
    for(sptr=klist.begin();sptr!=klist.end();++sptr)
    {
      if(mdef.writeable(*sptr)) clist.push_back(*sptr);
    }
    return(this->extract_selected(d,clist,verbose));
  }catch(...){throw;};
}
py::dict MongoDBConverter::all(const mspass::Metadata& d,bool verbose) const
{
  try{
    set<string> klist=d.keys();
    list<string> clist;
    set<string>::iterator sptr;
    for(sptr=klist.begin();sptr!=klist.end();++sptr)
    {
      clist.push_back(*sptr);
    }
    return(this->extract_selected(d,clist,verbose));
  }catch(...){throw;};
}
py::dict MongoDBConverter::selected(const mspass::Metadata& d,
  const py::list& keys, bool noabort,bool verbose ) const
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
    py::dict result=this->extract_selected(d,keystd,verbose);

    if(noabort)
      return result;
    else
    {
      int n_in,n_out;
      n_in=keystd.size();
      n_out=result.size();
      if(n_in!=n_out)
      {
        stringstream ss;
        ss << "MongoDBConverter::selected method: copy error"<<endl
          << "Input list of keys length="<<keys.size()<<endl
          << "Output python dict size="<<result.size()<<endl
          << "Assuming this is an interactive script"<<endl
          << "Run badkeys method to find problem, fix the list, and try again"
          <<endl;
          throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
      }
    }
    return result;
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
      else
      {
        if(!mdef.is_defined(keystr))
        {
          badlist.append(a);
        }
      }

    }
    return badlist;
  }catch(...){throw;};
}
}  // End mspass namespace encapsulation
