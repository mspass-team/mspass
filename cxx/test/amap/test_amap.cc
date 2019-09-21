#include "mspass/utility.h"
#include "mspass/AttributeMap.h"
#include "mspass/AttributeCrossReference.h"
using namespace mspass;
/* This is a test program for AttributeMap and AttributeCrossReference objects.*/

int main(int argc,char **argv)
{
    cout << "test_aml program:  testing AttributeMap and AttributeCrossRefence outility classes"<<endl
        << "Trying default constructor for AttributeMap"<<endl;
    AttributeMap amdef();
    cout << "Success: currently default map is for css3.0"<<endl
        << "Trying Trace4.0 - relic but ok for initial testing - change for release version"<<endl;
    string othername("Trace4.0");
    AttributeMap amother=new AttributeMap(othername);
    cout << "Trace4.0 was correctly loaded - testing destructor on the Trace4.0 data just loaded"<<endl;
    delete amother;
    cout << "Success"<<endl;
}
