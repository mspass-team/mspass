#include <boost/archive/text_oarchive.hpp>
#include "mspass/utility/ErrorLogger.h"
#include "mspass/utility/MsPASSError.h"
#include "mspass/utility/Metadata.h"
#include "mspass/utility/AntelopePf.h"
using namespace std;
using namespace mspass::utility;
void print_metadata(Metadata& md)
{
  ostringstream ss;
  ss << md;
  cout << ss.str();
}
int main(int argc, char **argv)
{
	char *pfname=strdup("test_md.pf");
	try {
            cout << "Test program for Metadata object, AntelopePf, and ErrorLogger"
                <<endl
            << "First try creating an ErrorLogger object"<<endl;
            ErrorLogger elog;
            cout << "Try algorithms to set jobid in ErrorLogger"<<endl;
            elog.set_job_id(10000);
            cout << "Success - job_id set to "<<elog.get_job_id()<<endl;
		cout << "Trying to build Metadata objects" << endl;
		cout << "Trying default constructor" << endl;
		Metadata mdplain;
                cout << "Trying put methods for int, double, string, and bool"
                    <<endl;
                long lval; double dval; string sval;  bool bval;
                lval=10;
                mdplain.put<long>("long_val",lval);
                dval=2.5;
                mdplain.put<double>("double_val",dval);
                sval=string("test_string");
                mdplain.put<string>("string_val",sval);
                bval=true;
                mdplain.put<bool>("bool_val",bval);
                cout << "Succeeded - trying matching get methods"<<endl;
                cout << "long_val="<<mdplain.get<long>("long_val")<<endl;
                cout << "double_val="<<mdplain.get<double>("double_val")<<endl;
                cout << "string_val="<<mdplain.get<string>("string_val")<<endl;
                cout << "bool_val="<<mdplain.get<bool>("bool_val")<<endl;
                cout << "Trying copy constructor followed by change of key string_val to sval"<<endl;
                Metadata mdctmp(mdplain);
                mdctmp.change_key("string_val","sval");
                cout << "Copy completed and change_key finished"<<endl;
                cout << "Value now associated with key sval="<<mdctmp.get<string>("sval")<<endl;
                cout << "Checking if old key was cleared - also tests is_defined method"<<endl;
                if(mdctmp.is_defined("string_val"))
                    cout << "BAD - string_val key is still defined - fix this bug"<<endl;
                else
                    cout << "string_val entry was cleared - success"<<endl;
		ostringstream ss;
		cout << "Trying to serialize with operator stringstream"<<endl;
		ss << mdplain;
		cout << "Succeeded - stringstream contents:"<<endl;
		cout << ss.str()<<endl;
		cout<< "Trying same with serialize_metadata function"<<endl;
		string sbuf=serialize_metadata(mdplain);
		cout<<"Serialized completed: content "<<endl
			<<"(should be same as stringstream output above)"<<endl;
		cout <<sbuf;
		cout << "Trying to run inverse function restore_serialized_metadata "
			<<"on sterialization output"<<endl;
		Metadata mrestored=restore_serialized_metadata(sbuf);
		cout<<"Result - should again be the same"<<endl;
		print_metadata(mrestored);
                cout << "Same thing using operator >> to cout"<<endl;
		print_metadata(mdplain);
		cout << "Testing is_defined and clear methods"<<endl;
		cout << "This should be a False(0) (undefined key)->"
			<<mdplain.is_defined("HUHLigh")<<endl;
		cout << "This should be a True(1)->"
			<<mdplain.is_defined("double_val")<<endl;
		/* Clear double_val*/
		mdplain.clear("double_val");
		if(mdplain.is_defined("double_val"))
			cout << "Test of clear method failed"<<endl;
		else
			cout << "Test of clear method succeeded"<<endl;
		cout << "Contents of edited mdplain"<<endl;
		print_metadata(mdplain);
		cout << "Trying simple file read constructor"<<endl
                    << "Reading from simple.txt"<<endl;
                ifstream ifs("simple.txt");
                Metadata mds(ifs);
		print_metadata(mds);
                cout << "Trying to read more complex pf file using AntelopePF object constructor"<<endl;
                AntelopePf pfsmd(pfname);
                cout << "Success - read the following:  "<<endl;
		print_metadata(pfsmd);
                cout << "Trying assignment operator for Metadata with RTTI"<<endl;
                Metadata mdsum;
                mdsum=dynamic_cast<Metadata&>(pfsmd);
                cout << "Worked"<<endl<<"Contents of copy (simple attributes ony)"<<endl;
		print_metadata(mdsum);
                cout << "Trying += operator.  Merging inline and pf objects"<<endl;
                cout << "Trying to add simple to Metadata derived from pf"<<endl;
                mdsum+=mds;
                cout << "Done - result:"<<endl;
		print_metadata(mdsum);
                cout << "Reading and writing a couple of simple parameters"<<endl;
                cout << "simple_real_parameter="
                    <<pfsmd.get<double>("simple_real_parameter")<<endl
                  << "simple_int_parameter="
                    <<pfsmd.get<int>("simple_int_parameter")<<endl
                  << "simple_bool_parameter="<<pfsmd.get_bool("simple_bool_parameter")
                  <<endl;
                cout << "Trying get_tbl method"<<endl;
                list<string> tsttbl;
                tsttbl=pfsmd.get_tbl("mdlist");
                cout << "mdlist Tbl extracted from pf"<<endl;
                list<string>::iterator iptr;
                for(iptr=tsttbl.begin();iptr!=tsttbl.end();++iptr)
                {
                  cout << *iptr<<endl;
                }
                cout << "Trying get_branch method - extracting test_nested_tag"
                  <<endl;
                AntelopePf pfbr(pfsmd.get_branch("test_nested_tag"));
                cout << "Success"<<endl
                    <<"Contents"<<endl;
		print_metadata(pfbr);
                cout << "test_double parameter in branch="<<pfbr.get_double("test_double")<<endl;
                cout << "Testing exceptions.  First a get failure:"<<endl;
                try{
                    double dbad=mdsum.get<double>("bad_key");
                    cout << "PROBLEM:  get did not throw error and returend"
                        << dbad<<endl;
                }catch(MetadataGetError& mdge)
                {
                    cout << "Properly handled.  Message posted follows:"<<endl
                        << mdge.what()<<endl;
                    if(mdge.severity() == ErrorSeverity::Invalid)
                        cout << "Correct severity of Invalid was posted"<<endl;
                    else 
                        cout << "Error - severity posted was not Invalid as expected"
                            <<endl;
                    cout << "Trying to write log with ErrorLogger"<<endl;
                    elog.log_error(mdge);
                    elog.log_verbose("test_md",string("log_error method succeeded"));
                }
                cout << "Trying intentional type mismatch."<<endl;
                try{
		    int ibad;
                    ibad=pfbr.get<int>("test_double");
		    cout<<"FAILURE:  returned a double as int="<<ibad;
                }catch(MetadataGetError& mdge)
                {
                    cout << "Properly handled trying to get test_double as int"
                        <<endl
                        <<"Error message posted"<<endl
                        << mdge.what()<<endl;
                    if(mdge.severity() == ErrorSeverity::Invalid)
                        cout << "Correct severity of Invalid was posted"<<endl;
                    else 
                        cout << "Error - severity posted was not Invalid as expected"
                            <<endl;
                    cout << "Posting that message to log"<<endl;
                    elog.log_error(mdge);
                }
                cout << "Posting a set of fake messages to log"<<endl;
                MsPASSError efatal(string("Fake fatal error"),"Fatal");
                elog.log_error(efatal);
                MsPASSError edebug(string("Fake debug error"),"Debug");
                elog.log_error(edebug);
                MsPASSError ec(string("Fake complain error"),"Complaint");
                elog.log_error(ec);
                list<LogData> ldata=elog.get_error_log();
                list<LogData>::iterator lptr;
                cout << "Dump of error log"<<endl;
                for(lptr=ldata.begin();lptr!=ldata.end();++lptr)
                {
                    cout << *lptr<<endl;
                }
		cout << "Testing serialization of error log"<<endl;
		stringstream serial_ss;
		boost::archive::text_oarchive ar(serial_ss);
		ar << elog;
		cout << "serialization finished"<<endl;
		cout << "Attempting to restore"<<endl;
		string serialbuf=serial_ss.str();
		istringstream eloginstrm(serialbuf);
		ErrorLogger elog_restored;
		boost::archive::text_iarchive arin(eloginstrm);
		arin>>elog_restored;
		cout << "Error log restored - contents should match above"<<endl;
		ldata=elog_restored.get_error_log();
		for(lptr=ldata.begin();lptr!=ldata.end();++lptr)
                {
                    cout << *lptr<<endl;
                }

	}
	catch (MsPASSError& sess)
	{
            cout << "Something threw an MsPASSError exception - message posted follows"<<endl;
	    cout << sess.what()<<endl;
            if(sess.severity() == ErrorSeverity::Invalid)
                cout << "Correct severity of Invalid was posted"<<endl;
            else 
                cout << "Error - severity posted was not Invalid as expected"
                    <<endl;
	}
        catch(std::exception& stex)
        {
            cout << "Something threw a std::exception that was not a MsPASSError"<<endl
                << "Error message:  "<<stex.what()<<endl;
	
        }
}
