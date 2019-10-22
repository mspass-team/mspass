#include "mspass/utility/utility.h"
#include "mspass/utility/AntelopePf.h"
#include "mspass/utility/AttributeMap.h"
#include "mspass/utility/AttributeCrossReference.h"
using namespace mspass;
/* This is a test program for AttributeMap and AttributeCrossReference objects.*/

int main(int argc,char **argv)
{
  try{
    int retcode(0);
    cout << "test_aml program:  testing AttributeMap and AttributeCrossRefence outility classes"<<endl
        << "Trying default constructor for AttributeMap"<<endl;
    AttributeMap amdef;
    cout << "Success: currently default map is for css3.0"<<endl
        << "Trying Trace4.0 - relic but ok for initial testing - change for release version"<<endl;
    string othername("Trace4.0");
    AttributeMap *amother=new AttributeMap(othername);
    cout << "Trace4.0 was correctly loaded - testing destructor on the Trace4.0 data just loaded"<<endl;
    delete amother;
    cout << "Success"<<endl;
    cout << "Testing operator="<<endl;
    amdef=AttributeMap("css3.0");
    cout << "Success"<<endl<<"Testing operator[] on a few common attributes"<<endl;
    cout << "Note this also tests operator<< for AttributeProperties"<<endl;
    AttributeProperties ap;
    ap=amdef[string("arrival.time")];
    cout << ap;
    ap=amdef["event.evid"];
    cout << ap;
    ap=amdef["site.lon"];
    cout << "Testing is_alias method"<<endl;
    bool bval;
    bval=amdef.is_alias("sta");
    if(bval) 
        cout << "Correctly returned true for attribute sta"<<endl;
    else
    {
        cout << "ERROR - returned false for sta - should be true"<<endl;
        ++retcode;
    }
    bval=amdef.is_alias("foo"); 
    if(bval)
    {
        cout << "ERROR - returned true for invalid key - should be false"<<endl;
        ++retcode;
    }
    else
        cout << "Corrected returned false for invalid key"<<endl;
    cout << "Testing aliastable method"<<endl<<"Trying to retrieve table for attribute time"<<endl;
    list<string> atbl=amdef.aliastables("time");
    cout << "Success - here is the list of aliases returned"<<endl;
    list<string>::iterator sptr;
    for(sptr=atbl.begin();sptr!=atbl.end();++sptr)
        cout << *sptr<<endl;
    cout << "Testing aliases method for attribute phase"<<endl;
    map<string,AttributeProperties> amap=amdef.aliases("phase");
    cout << "Success - map returned by aliases has "<<amap.size()<<" entries"<<endl;
    cout << "Example arrival.time"<<endl;
    cout << amdef["arrival.time"]<<endl;
    cout << "Starting tests of AttributeCrossReference - trying to read test pf file"<<endl;
    string dir=mspass::data_directory();
    dir=dir+"/pf";
    string dfile=dir+"/axref_test.pf";
    AntelopePf pf(dfile);
    list<string> xreflist=pf.get_tbl("metadata_cross_reference");
    cout << "Successfully read test data file"<<endl
        << "Trying list container constructor"<<endl;
    AttributeCrossReference axref(xreflist);
    cout << "Success:   input map size="<<axref.size()<<endl;
    cout << "Testing copy constructor"<<endl;
    AttributeCrossReference *axcpy=new AttributeCrossReference(axref);
    if(axref.size()==axcpy->size())
        cout << "Size of copy matches original - assume success"<<endl;
    else
    {
        cout << "Something is wrong.   Size of copy="<<axcpy->size()<<endl;
        ++retcode;
    }
    cout << "Testing destructor"<<endl;
    delete axcpy;
    cout << "Testing operator="<<endl;
    AttributeCrossReference axcpy2=axref;
    if(axref.size()==axcpy2.size())
        cout << "Size of copy matches original - assume success"<<endl;
    else
    {
        cout << "Something is wrong.   Size of copy="<<axcpy2.size()<<endl;
        ++retcode;
    }
    cout << "Testing internal method"<<endl;
    const string ekey("sourceLongOrX");
    const string ikey("sx");
    string stest;
    stest=axref.internal(ekey);
    cout << "Internal method returned "<<stest;
    if(stest==ikey) 
        cout <<"Test passed - got result expected"<<endl;
    else
    {
        cout << "ERROR:   expected result="<<ikey;
        ++retcode;
    }
    cout << "Testing external method"<<endl;
    stest=axref.external(ikey);
    cout << "Internal method returned "<<stest;
    if(stest==ekey) 
        cout <<"Test passed - got result expected"<<endl;
    else
    {
        cout << "ERROR:   expected result="<<ekey;
        ++retcode;
    }
    exit(retcode);

  }catch(MsPASSError& merr)
  {
      cerr << "Error - here is the MsPASSException message:"<<endl<<merr.what()<<endl;
  }
  catch(std::exception& oerr)
  {
      cerr << "Something else threw this std::exception"<<endl<<oerr.what()<<endl;
  }
  catch(...)
  {
      cerr << "Something threw an unexpected exception"<<endl;
  }
  exit(-1);
}
