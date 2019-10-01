#include "mspass/MsPASSError.h"
#include "mspass/Metadata.h"
#include "mspass/AntelopePf.h"
using namespace std;
using namespace mspass;
namespace mspass
{
MetadataList get_mdlist(const AntelopePf& m,const string tag)
{
    try {
        MetadataList mdl;
	Metadata_typedef mdt;	
	string mdname, mdtype;
        list<string> t=m.get_tbl(tag);
        list<string>::iterator tptr;
        for(tptr=t.begin();tptr!=t.end();++tptr)
        {
            /* This loop is identical to above in pfget_mdlist
               except for the next line*/
		istringstream instr(*tptr);
		instr >> mdname;
		instr >> mdtype;
		mdt.tag = mdname;
		if(mdtype=="real" || mdtype=="REAL" || mdtype=="float" 
                            || mdtype=="FLOAT" || mdtype=="Real32")
			mdt.mdt = MDtype::Real;
		else if(mdtype=="double" || mdtype=="Real64" || mdtype=="real64")
			mdt.mdt = MDtype::Double;
		else if(mdtype=="int" || mdtype=="INT" || mdtype=="integer")
			mdt.mdt = MDtype::Integer;
		else if(mdtype=="string" || mdtype=="STRING")
			mdt.mdt = MDtype::String;
		else if(mdtype=="boolean" || mdtype=="BOOLEAN")
			mdt.mdt = MDtype::Boolean;
                else
                {
                    stringstream ss;
                    ss << "get_mdlist:  Error in mdlist specification type name defined by key = "
                            << mdtype << " was not recognized."<<endl
                            << "Listed with attribute name="
                            << mdname <<endl<<"List is not valid."<<endl;
                    /* We mark this invalid, but it will probably normally be a fatal error 
                       because this procedure would normally be calld on startup */
                    throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
                }
		mdl.push_back(mdt);
	}
        return(mdl);
    }catch(...){throw;};
}
int  copy_selected_metadata(const Metadata& mdin, Metadata& mdout,
    const MetadataList& mdlist)
{
    MetadataList::const_iterator mdti;
    int count;

    for(mdti=mdlist.begin(),count=0;mdti!=mdlist.end();++mdti,++count)
    {
        MDtype mdtest;
        float f;
        double r;
        int ii;
        long iv;
        string s;
        bool b;

        mdtest = mdti->mdt;
        try
        {
            switch(mdtest)
            {
                case MDtype::Real:
                case MDtype::Real32:
                    f=mdin.get<float>(mdti->tag);
                    mdout.put(mdti->tag,f);
                    ++count;
                    break;
                case MDtype::Double:
                case MDtype::Real64:
                    r=mdin.get<double>(mdti->tag);
                    mdout.put(mdti->tag,r);
                    ++count;
                    break;
                case MDtype::Integer:
                case MDtype::Int32:
                    ii=mdin.get<int>(mdti->tag);
                    mdout.put(mdti->tag,ii);
                    ++count;
                    break;
                case MDtype::Long:
                case MDtype::Int64:
                    iv=mdin.get<long>(mdti->tag);
                    mdout.put(mdti->tag,iv);
                    ++count;
                    break;
                case MDtype::String:
                    s=mdin.get<string>(mdti->tag);
                    mdout.put(mdti->tag,s);
                    ++count;
                    break;
                case MDtype::Boolean:
                    b=mdin.get<bool>(mdti->tag);
                    mdout.put(mdti->tag,b);
                    ++count;
                    break;
                case MDtype::Invalid:
                    // silently skip values marked as invalid
                    break;
                default:
                    throw MsPASSError(string("copy_selected_metadata: ")
				+ " was passed illegal type definition\n"
				+ string("This indicates a coding error that must be fixed\n")
				+ string("If caller does not exit on this error, expect a less graceful abort"),
                                ErrorSeverity::Fatal);
            };
        } catch( ... )
        {
            throw;
        }
    }
    return count;
}
} // Termination of namespace SEISPP definitions

