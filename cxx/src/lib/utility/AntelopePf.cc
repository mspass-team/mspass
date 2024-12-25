#include <ctype.h>
#include <unistd.h>
#include <sys/stat.h>
#include <string>
#include <list>
#include <iostream>
#include <fstream>
#include <sstream>
#include "mspass/utility/MsPASSError.h"
#include "mspass/utility/utility.h"
#include "mspass/utility/AntelopePf.h"
namespace mspass::utility {
using namespace std;
using namespace mspass::utility;
enum PfStyleInputType {PFMDSTRING, PFMDREAL, PFMDINT, PFMDBOOL, PFMDARR, PFMDTBL};
/* this is functionally similar to antelope yesno function, but
   in a more C++ style.  int return code is the same:
   0 is false, -1 for boolean true, and +1 for no match.
   One difference from antelope is that 0 or 1 for the true or
   false value will yield a -1.  That approach conflicts with
   any notion of type as there is no way to tell these from
   an int.   Problem for words too, but the words are not likely
 to be used in another context while 0 is a very common parameter value.
 This feature should not cause harm anyway because all simple
 parameters get echoed to the string map anyway.*/
int yesno(string s)
{
    if( (s=="yes") || (s=="ok") || (s=="y") || (s=="true")
            || (s=="on") || (s=="t") ) return(-1);
    if( (s=="no") || (s=="n") || (s=="false") || (s=="off")
            || (s=="f") ) return(0);
    return(1);
}
/* This internal helper tests an std::string to make a guess of
   the type.   The algorithm is:
    if( contains &Arr ) return Arr
    else if (contains &Tbl) return Tbl
    else
      enter algorithm to parse simple tokens

    The else clause defines a real as containing a ".",
    a "-", and/or a e/E.
    */
PfStyleInputType arg_type(string token)
{
    const string arrtag("&Arr");
    const string tbltag("&Tbl");
    const string period(".");
    const string expchars("eE");
    size_t found;
    found=token.find(arrtag);
    if(found!=string::npos) return(PFMDARR);
    found=token.find(tbltag);
    if(found!=string::npos) return(PFMDTBL);
    if(yesno(token)!=1) return(PFMDBOOL);
    int slen=token.size();
    int i;
    bool found_an_e(false);
    for(i=0; i<slen; ++i)
    {
        /* This will fail if one had an odd key like 0_0 but
           would at a lot of complexity to allow that.
           It also should skip negative sign. */
        if(isalpha(token[i]))
        {
            if((token[i]=='e') || (token[i]=='E'))
            {
                if(found_an_e)
                    return(PFMDSTRING);
                else
                    found_an_e=true;
            }
            else
                return(PFMDSTRING);
        }
    }
    /* If we got here we can assume token is numbers.  Now decide
       if it is real or int by presence of a decimal point*/
    found=token.find(period);
    if(found!=string::npos) return(PFMDREAL);
    found=token.find(expchars);
    if(found!=string::npos) return(PFMDREAL);
    return(PFMDINT);
}
/* returns true if the first nonwhite space character is a # sign*/
bool is_comment_line(string testline)
{
    const string white(" \t\n");
    size_t start;
    start=testline.find("#");
    if(start==string::npos) return(false);
    /* We define a comment line as one with nothing else
       Complexity here to deal with leading white space */
    start=testline.find_first_not_of(white,0);
    if(start!=0) testline.erase(0,start);
    char c=testline[0];
    if(c=='#')
        return true;
    else
        return false;
}
AntelopePf pfread(const string fname)
{
    const string base_error("mspass::utility::pfread:  ");
    /* Low level check to see if file exists - for some reason ifstream
     * does not throw an error if trying to read from a nonexistent file.
     * Seems like a mistake in std to me. Taken from example on web
     * man page for stat doesn't explicitly say this, but example shows
     * that stat will return nonzero if file does not exist*/
    struct stat buffer;
    if(stat(fname.c_str(),&buffer))
    {
      throw MsPASSError(base_error+"file="+fname+" does not exist");
    }
    ifstream inp;
    inp.open(fname.c_str(),ios::in);
    if(inp.fail())
        throw MsPASSError(base_error + "open failed for file="+fname);

    /* Eat up the whole file - this assumes not a large
       file following antelope pf model.  */
    list<string> alllines;
    char inbuffer[512];
    while(inp.getline(inbuffer,512))
    {
        const string white(" \n\t");
        // Probably should test for overflow right here - do not know how
        string rawline(inbuffer);
        if(rawline.find_first_not_of(white,0)==string::npos) continue;
        if(is_comment_line(rawline)) continue;
        alllines.push_back(rawline);
    }
    try {
        AntelopePf result(alllines);
        return(result);
    }
    catch(...) {
        throw;
    };
}

/* This is a helper for the primary constructor below.   It searches
   for closing curly bracket.  It returns a count of the number of
   lines that define this block starting from the iterator first.
   */
int find_end_block(list<string>& alllines,list<string>::iterator first)
{
    try {
        int level(0);
        int count;
        list<string>::iterator current,lastline;
        lastline=alllines.end();
        for(count=0,current=first; current!=lastline; ++current)
        {
            ++count;
            if(current->find("{")!=string::npos) ++level;
            if(current->find("}")!=string::npos) --level;
            if(level==0) break;
        }
        return(count);
    } catch(...) {
        throw;
    };
}
pair<string,string> split_line(string s)
{
    const string white(" \t");
    const string terminators("\n#");
    size_t is,ie;
    is=s.find_first_not_of(white,0);
    ie=s.find_first_of(white,is);
    string key;
    key.assign(s,is,ie-is);
    is=s.find_first_not_of(white,ie);
    ie=s.find_first_of(terminators,is);
    string val;
    val.assign(s,is,ie-is);
    pair<string,string> result;
    result.first=key;
    result.second=val;
    return result;
}
/* Note this constructor is recursive.  That is when there is
   a nest &Arr{ in a pf this constructor will call itself for
   each Arr block */
AntelopePf::AntelopePf(list<string> alllines) : Metadata()
{
    const string base_error("AntelopePf constructor:  ");
    list<string>::iterator lptr,end_block;
    list<string> block;
    string key,token2;
    try {
        for(lptr=alllines.begin(); lptr!=alllines.end(); ++lptr)
        {
            int i,ival;
            int lines_this_block;
            /* This is redundant, but cost is low for long term stability*/
            if(is_comment_line(*lptr)) continue;
            /* Older version used a stringstream here but it would not
               parse string attributes with embedded white space.  This
               revised algorithm will do that.  returned pair has the
               key as first and the string with the key trimmed in second. */
            /*
            istringstream is(*lptr);
            is>>key;
            is>>token2;
            */
            pair<string,string> sl=split_line(*lptr);
            key=sl.first;
            token2=sl.second;
            PfStyleInputType type_of_this_line=arg_type(token2);
            switch(type_of_this_line)
            {
            /* Note all simple type variables are duplicated in
               as strings.  This is a failsafe mechanism used
               because Metadata will always fall back to string
               map if the type specific versions fail.   Realize
               if Metadata changes this will be wasted effort. */
            case  PFMDSTRING:
                this->put(key,token2);
                break;
            case PFMDREAL:
                this->put(key,token2);
                this->put(key,atof(token2.c_str()));
                break;
            case PFMDINT:
                /* save ints as both string and int some string
                   values could be all digits. */
                this->put(key,token2);
                this->put(key,atoi(token2.c_str()));
                break;
            case PFMDBOOL:
                ival=yesno(token2);
                if(ival!=0)
                    this->put<bool>(key,true);
                else
                    this->put<bool>(key,false);
                break;
            case PFMDTBL:
            case PFMDARR:
                lines_this_block=find_end_block(alllines,lptr);
                block.clear();
                /* Skips first and last lines in this loop.  first with
                   if and last with termination condition.  Note this
                   will cause problem if a curly back is not alone on
                   the last line */
                for(i=0; i<(lines_this_block-1); ++i,++lptr)
                    if(i>0)block.push_back(*lptr);
                if(type_of_this_line==PFMDTBL)
                    pftbls.insert(pair<string,list<string> >(key,block));
                else
                {
                    AntelopePf thispfarr(block);
                    pfbranches.insert(pair<string,AntelopePf>
                                      (key,thispfarr));
                }
                break;
            default:
                throw AntelopePfError(base_error
                                      +"Error parsing this line->"
                                      +*lptr);
            }
        }
    } catch(...) {
        throw;
    };
}
/* This is the private helper function for the PFPATH constructor */
int AntelopePf::merge_pfmf(AntelopePf& m)
{
    try {
        /* Downcast both the current object and m to Metadata and then we
         * can use the += operator.  Laziness stops me from making this method
         * the += operator as that is more or less what it does. */
        Metadata *tptr=dynamic_cast<Metadata*>(this);
        Metadata *mmmd=dynamic_cast<Metadata*>(&m);
        *tptr += (*mmmd);
        /* Now we accumulate any Arr and Tbl entries. */
        int count(0);
        list<string> akey,tkey;
        akey=m.arr_keys();
        tkey=m.tbl_keys();
        list<string>::iterator kptr;
        for(kptr=tkey.begin(); kptr!=tkey.end(); ++kptr)
        {
            list<string> t=m.get_tbl(*kptr);
            this->pftbls[*kptr]=t;
            ++count;
        }
        for(kptr=akey.begin(); kptr!=akey.end(); ++kptr)
        {
            AntelopePf pfv;
            pfv=m.get_branch(*kptr);
            this->pfbranches[*kptr]=pfv;
            ++count;
        }
        return count;
    } catch(...) {
        throw;
    };
}
/* small helper - splits s into tokens and assures each result is
is a pf file with the required .pf ending */
list<string> split_pfpath(string pfbase, char *s)
{
    /* make sure pfbase ends in .pf.  If it doesn't, add it */
    string pftest(".pf");
    std::size_t found;
    found=pfbase.find(pftest);
    if(found==std::string::npos) pfbase+=pftest;
    list<string> pftmp;
    char *p;
    p=strtok(s,":");
    while(p!=NULL)
    {
        pftmp.push_back(string(p));
        p=strtok(NULL,":");
    }
    list<string> pfret;
    list<string>::iterator pfptr;
    for(pfptr=pftmp.begin(); pfptr!=pftmp.end(); ++pfptr)
    {
        string fname;
        fname=(*pfptr)+"/"+pfbase;
        pfret.push_back(fname);
    }
    return pfret;
}
AntelopePf::AntelopePf(string pfbase)
{
    try {
        list<string> pffiles;
        const std::string mspass_home_envname("MSPASS_HOME");
        char *base;
        /* Note man page for getenv says explicitly the return of getenv should not
                be touched - i.e. don't free it*/
        base=getenv(mspass_home_envname.c_str());
        if(base!=NULL)
        {
            pffiles.push_back(data_directory()+"/pf/"+pfbase);
        }
        const string envname("PFPATH");
        char *s=getenv(envname.c_str());
        if(s==NULL)
        {
            pffiles.push_back(pfbase);
        }
        /* Test to see if pfbase is an absolute path - if so just use
         * it and ignore the pfpath feature */
        else if(pfbase[0]=='/')
        {
            pffiles.push_back(pfbase);
        }
        else
        {
            pffiles=split_pfpath(pfbase,s);
        }
        list<string>::iterator pfptr;
        int nread;

        for(nread=0,pfptr=pffiles.begin(); pfptr!=pffiles.end(); ++pfptr)
        {
            // Skip pf files that do not exist
            if(access(pfptr->c_str(),R_OK)) continue;
            if(nread==0)
            {
                *this=pfread(*pfptr);
            }
            else
            {
                AntelopePf pfnext=pfread(*pfptr);
                this->merge_pfmf(pfnext);
            }
            ++nread;
        }
        if(nread==0) 
        {
          if(s==NULL)
          {
            // This was necessary to avoid seg faults when s was a null pointer
            // which happens when PFPATH is not set 
            throw MsPASSError(string("PFPATH is not defined and default pf=")
               + pfbase + " was not found",ErrorSeverity::Invalid);
          }
          else
          {
            throw MsPASSError(string("PFPATH=")+s
             +" had no pf files matching " + pfbase,ErrorSeverity::Invalid);
          }
        }
    } catch(...) {
        throw;
    };
}

AntelopePf::AntelopePf(const AntelopePf& parent)
    : Metadata(parent)
{
    pftbls=parent.pftbls;
    pfbranches=parent.pfbranches;
}
list<string> AntelopePf::get_tbl(const string key) const
{
    map<string,list<string> >::const_iterator iptr;
    iptr=pftbls.find(key);
    if(iptr==pftbls.end()) throw AntelopePfError(
            "get_tbl failed trying to find data for key="+key);
    return(iptr->second);
}
AntelopePf AntelopePf::get_branch(const string key) const
{
    map<string,AntelopePf>::const_iterator iptr;
    iptr=pfbranches.find(key);
    if(iptr==pfbranches.end()) throw AntelopePfError(
            "get_branch failed trying to find data for key="+key);
    return iptr->second;
}
list<string> AntelopePf::arr_keys() const
{
    map<string,AntelopePf>::const_iterator iptr;
    list<string> result;
    for(iptr=pfbranches.begin(); iptr!=pfbranches.end(); ++iptr)
        result.push_back((*iptr).first);
    return result;
}
list<string> AntelopePf::tbl_keys() const
{
    map<string,list<string> >::const_iterator iptr;
    list<string> result;
    for(iptr=pftbls.begin(); iptr!=pftbls.end(); ++iptr)
        result.push_back((*iptr).first);
    return(result);
}
AntelopePf& AntelopePf::operator=(const AntelopePf& parent)
{
    if(this!=&parent)
    {
        /* This uses a trick to use operator = of the parent object */
        this->Metadata::operator=(parent);
        pftbls=parent.pftbls;
        pfbranches=parent.pfbranches;
    }
    return(*this);
}
Metadata AntelopePf::ConvertToMetadata()
{
    try {
        Metadata result(dynamic_cast<Metadata&>(*this));
        boost::any aTbl=this->pftbls;
        //boost::any<map<string,list<string>> aTbl(this->pftbl);
        boost::any aArr=this->pfbranches;
        result.put<boost::any>(pftbl_key,aTbl);
        result.put<boost::any>(pfarr_key,aArr);
        return result;
    } catch(...) {
        throw;
    };
}

} // End mspass::utility Namespace declaration
