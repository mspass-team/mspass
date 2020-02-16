#include <iomanip>
#include <boost/core/demangle.hpp>
#include "mspass/utility/Metadata.h"
namespace mspass
{
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
void Metadata::clear(const std::string key)
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
        os<<mdptr->first<<" "<<sname<<" ";
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
                os<<sval<<endl;
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
/* This function is very much like operator<< except it is more
 * restricted on allowed types and it add a type name to the output */
std::string serialize_metadata(const Metadata& md)
{
  try{
    ostringstream ss;
    /* We do this to make sure we don't truncate precision */
    ss<<setprecision(14);
    ss << const_cast<Metadata&>(md);
    return std::string(ss.str());
  }catch(...){throw;};
}
/* This has a lot more complexity but assumes a series of lines
 * defined by ostringstream operator:  key, type, value
 * */
Metadata restore_serialized_metadata(const std::string s)
{
  try{
    stringstream ss(s);
    Metadata md;
    string key,typ;
    double dval;
    long int ival;
    bool bval;
    string sval;
    do{
      ss>>key;
      ss>>typ;
      if(ss.eof())break;   // normal exit of this loop is here
      if(typ=="double")
      {
        ss>>dval;
        md.put(key,dval);
      }
      else if( (typ=="long")||(typ=="int") )
      {
        ss>>ival;
        md.put(key,ival);
      }
      else if(typ=="bool")
      {
        ss>>bval;
        md.put(key,bval);
      }
      /* this assumes output has been made pretty so this simple test works*/
      else if(typ=="string")
      {
        ss>>sval;
        md.put(key,sval);
      }
      else
      {
        cerr << "restore_serialized (WARNING):  unrecognized type for key="<<key
           << " of "<<typ<<endl<<"Trying to save as string"<<endl;
        ss>>sval;
        md.put(key,sval);
      }
    }while(!ss.eof());
    return md;
  }catch(...){throw;};
}

} // End mspass Namespace block
