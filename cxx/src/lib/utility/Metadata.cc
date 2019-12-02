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
set<string> Metadata::keys() noexcept
{
  set<string> result;
  map<string,boost::any>::iterator mptr;
  for(mptr=md.begin();mptr!=md.end();++mptr)
  {
    string key(mptr->first);\
    result.insert(key);
  }
  return result;
}
/* Helper returns demangled name using boost demangle.  */
string demangled_name(boost::any a)
{
    try{
        const std::type_info &ti = a.type();
        const char *rawname=ti.name();
        string pretty_name(boost::core::demangle(rawname));
        return pretty_name;
    }catch(...){throw;};
}
ostream& operator<<(ostream& os, Metadata& m)
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

} // End mspass Namespace block
