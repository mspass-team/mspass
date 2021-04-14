#include <iomanip>
#include <boost/core/demangle.hpp>
#include <pybind11/stl.h>
#include "misc/base64.h"
#include "mspass/utility/Metadata.h"
#include "mspass/utility/MsPASSError.h"
namespace mspass::utility
{
using namespace std;

Metadata::Metadata(ifstream& ifs, const string form)
{
  try{
    char linebuffer[256];
    while(ifs.getline(linebuffer,128))
    {
      string s1,s2,s3;
      boost::any a;
      stringstream ss(linebuffer);
      ss>>s1;
      ss>>s2;
      ss>>s3;
      if(s3=="real")
      {
        double dval;
        dval=atof(s2.c_str());
        a=dval;
        md[s1]=a;
        changed_or_set.insert(s1);
      }
      else if(s3=="integer")
      {
        long ival;
        ival=atol(s2.c_str());
        a=ival;
        md[s1]=a;
        changed_or_set.insert(s1);
      }
      else if(s3=="string")
      {
        string sval;
        a=sval;
        md[s1]=a;
        changed_or_set.insert(s1);
      }
      else if(s3=="bool")
      {
        bool bval;
        if( (s2=="TRUE")||(s2=="true")||(s2=="1") )
          bval=true;
        else
          bval=false;
        a=bval;
        md[s1]=a;
        changed_or_set.insert(s1);
      }
      else
      {
        stringstream sserr;
        sserr << "Metadata file constructor:  Illegal type specification for key="
          << s1<<" with a value field of "<<s2<<endl
          << "type specified as "<<s3<<" is illegal.  "
          << "Must be on of the following:  real, integer, bool, or string."<<endl;
        throw MsPASSError(sserr.str(),ErrorSeverity::Invalid);
      }
    }
  }catch(...){throw;};
}
Metadata::Metadata(const Metadata& parent)
  : md(parent.md),changed_or_set(parent.changed_or_set)
{
}
bool Metadata::is_defined(const string key) const noexcept
{
  map<string,boost::any>::const_iterator mptr;
  mptr=md.find(key);
  if(mptr!=md.end())
  {
    return true;
  }
  else
  {
    return false;
  }
}
void Metadata::append_chain(const std::string key, const std::string val,
                  const std::string separator)
{
  if(this->is_defined(key))
  {
    string typ=this->type(key);
    if(typ.find("string")==string::npos)
	    throw MsPASSError("Metadata::append_chain:  data for key="
	      + key + " is not string type but "+typ
	      + "\nMust be string type to define a valid chain",
	      ErrorSeverity::Invalid);
    string sval=this->get_string(key);
    sval += separator;
    sval += val;
    this->put(key,sval);
  }
  else
  {
    this->put(key,val);
  }
  changed_or_set.insert(key);
}
Metadata& Metadata::operator=(const Metadata& parent)
{
  if(this!=(&parent))
  {
    md=parent.md;
    changed_or_set=parent.changed_or_set;
  }
  return *this;
}

Metadata& Metadata::operator+=(const Metadata& rhs) noexcept
{
  if(this!=(&rhs))
  {
    /* We depend here upon the map container replacing values associated with
    existing keys.  We mark all entries changes anyway.  This is a vastly simpler
    algorithm than the old SEISPP::Metadata.  */
    map<string,boost::any>::const_iterator rhsptr;
    for(rhsptr=rhs.md.begin();rhsptr!=rhs.md.end();++rhsptr)
    {
      md[rhsptr->first]=rhsptr->second;
      changed_or_set.insert(rhsptr->first);
    }
  }
  return *this;
}
const Metadata Metadata::operator+(const Metadata& other) const
{
  Metadata result(*this);
  result += other;
  return result;
}
set<string> Metadata::keys() const noexcept
{
  set<string> result;
  map<string,boost::any>::const_iterator mptr;
  for(mptr=md.begin();mptr!=md.end();++mptr)
  {
    string key(mptr->first);\
    result.insert(key);
  }
  return result;
}
void Metadata::erase(const std::string key)
{
  map<string,boost::any>::iterator iptr;
  iptr=md.find(key);
  if(iptr!=md.end())
    md.erase(iptr);
  /* Also need to modify this set if the key is found there */
  set<std::string>::iterator sptr;
  sptr=changed_or_set.find(key);
  if(sptr!=changed_or_set.end())
	  changed_or_set.erase(sptr);
}
std::size_t Metadata::size() const noexcept
{
  return md.size();
}
std::map<string,boost::any>::const_iterator  Metadata::begin() const noexcept
{
  return md.begin();
}
std::map<string,boost::any>::const_iterator  Metadata::end() const noexcept
{
  return md.end();
}

/* Helper returns demangled name using boost demangle.  */
string demangled_name(const boost::any a)
{
    try{
        const std::type_info &ti = a.type();
        const char *rawname=ti.name();
        string pretty_name(boost::core::demangle(rawname));
        return pretty_name;
    }catch(...){throw;};
}
std::string Metadata::type(const string key) const
{
    try{
      boost::any a=this->get_any(key);
      return demangled_name(a);
    }
    catch(...){throw;};
}
/* friend operator */
ostringstream& operator<<(ostringstream& os, Metadata& m)
{
  try{
    map<string,boost::any>::iterator mdptr;
    for(mdptr=m.md.begin();mdptr!=m.md.end();++mdptr)
    {
        /* Only handle simple types for now.  Issue an error message to
         * cerr for other types */
        int ival;
        long lval;
        double dval;
        float fval;
        string sval;
        bool bval;
        pybind11::object poval;
        boost::any a=mdptr->second;
        /* A relic retained to help remember this construct*/
        //const std::type_info &ti = a.type();
        string pretty_name=demangled_name(a);
        /*WARNING:  potential future maintance issue.
         * Currently the demangling process is not standardized and the
         * boost code used here does not return a name that is at all
         * pretty for string data.   This crude approach just tests for
         * the keyword basic_string embedded in the long name.  This works
         * for now, but could create problems if and when this anomaly
         * evolves away. */
        string sname("string");
        if(pretty_name.find("basic_string")==std::string::npos)
            sname=pretty_name;
        os<<misc::base64_encode(mdptr->first.c_str(), mdptr->first.size())<<" "<<sname<<" ";
        try{
            if(sname=="int")
            {
                ival=boost::any_cast<int>(a);
                os<<ival<<endl;
            }
            else if(sname=="long")
            {
                lval=boost::any_cast<long int>(a);
                os<<lval<<endl;
            }
            else if(sname=="double")
            {
                dval=boost::any_cast<double>(a);
                os<<dval<<endl;
            }
            else if(sname=="float")
            {
                fval=boost::any_cast<float>(a);
                os<<fval<<endl;
            }
            else if(sname=="bool")
            {
                bval=boost::any_cast<bool>(a);
                os<<bval<<endl;
            }
            else if(sname=="string")
            {
                sval=boost::any_cast<string>(a);
                string code = misc::base64_encode(sval.c_str(), sval.size());
                os<<code<<endl;
            }
            else if(sname=="pybind11::object")
            {
                poval=boost::any_cast<pybind11::object>(a);
                pybind11::gil_scoped_acquire acquire;
                pybind11::module pickle = pybind11::module::import("pickle");
                pybind11::module base64 = pybind11::module::import("base64");
                pybind11::object dumps = pickle.attr("dumps");
                pybind11::object b64encode = base64.attr("b64encode");
                /* The following in Python will be base64.b64encode(pickle.dumps(poval)).decode()
                 * The complexity is to ensure the bytes string to be valid UTF-8 */
                pybind11::object pyStr = b64encode(dumps(poval)).attr("decode")();
                os<<pyStr.cast<std::string>()<<endl;
                pybind11::gil_scoped_release release;
            }
            else
            {
                os <<"NONPRINTABLE"<<endl;
            }
        }catch(boost::bad_any_cast &e)
        {
            os<<"BAD_ANY_CAST_ERROR"<<endl;
        }
    }
    return os;
  }catch(...){throw;};
}
/* This function is implemented with pybind11 so it should only be called under python. */
pybind11::object serialize_metadata(const Metadata &md)
{
  pybind11::gil_scoped_acquire acquire;
  try{
    pybind11::dict md_dict = pybind11::cast(md);
    pybind11::set changed_or_set = pybind11::cast(md.changed_or_set);
    pybind11::module pickle = pybind11::module::import("pickle");
    pybind11::object dumps = pickle.attr("dumps");
    pybind11::object md_dump = dumps(pybind11::make_tuple(md_dict, changed_or_set));
    pybind11::gil_scoped_release release;
    return md_dump;
  }catch(...){pybind11::gil_scoped_release release;throw;};
}
/* This is the reverse of serialize_metadata also using pybind11. */
Metadata restore_serialized_metadata(const pybind11::object &s)
{
  pybind11::gil_scoped_acquire acquire;
  try{
    pybind11::module pickle = pybind11::module::import("pickle");
    pybind11::object loads = pickle.attr("loads");
    pybind11::tuple md_dump = loads(s);
    pybind11::dict md_dict = md_dump[0];
    pybind11::object md_py = pybind11::module_::import("mspasspy.ccore.utility").attr("Metadata")(md_dict);
    Metadata md = md_py.cast<Metadata>();
    md.changed_or_set = md_dump[1].cast<std::set<std::string>>();
    pybind11::gil_scoped_release release;
    return md;
  }catch(...){pybind11::gil_scoped_release release;throw;};
}
/* New method added Apr 2020 to change key assigned to a value - used for aliass*/
void Metadata::change_key(const string oldkey, const string newkey)
{
  map<string,boost::any>::iterator mdptr;
  mdptr=md.find(oldkey);
  /* We silently do nothing if old is not found */
  if(mdptr!=md.end())
  {
    md.insert_or_assign(newkey, mdptr->second);
    md.erase(mdptr);
  }
}
} // End mspass::utility Namespace block
