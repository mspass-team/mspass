#include <iostream>
#include "mspass/utility/dmatrix.h"
using namespace std;
using namespace mspass;
dmatrix MakeA()
{
    dmatrix A(3,2);
    A(0,0)=2.0;
    A(1,0)=3.0;
    A(2,0)=-1.0;
    A(0,1)=4.0;
    A(1,1)=-2.0;
    A(2,1)=6.0;
    return A;
}
dvector Make_x()
{
    dvector x(2);
    x(0)=1.0;
    x(1)=-1.0;
    return x;
}
int main(int argc, char **argv)
{
    try{
        cout << "Test program for dmatrix class"<<endl;
        cout << "Calling procedure to generate matrix A"<<endl
            << "This tests initializing constructor, operator(), "
            << "copy constructor, and operator="<<endl;
        dmatrix A;
        A=MakeA();
        cout << "Success:  printing result to test operator <<"<<endl;
        cout<< A<<endl;
        cout << "Testing size methods"<<endl;
        cout << "Number of columns in A="<<A.columns()<<endl;
        cout << "Number of rows in A="<<A.rows()<<endl;
        cout << "This should be the output of the size operator as a dvector"
            <<endl;
        vector<int> sizeA;
        sizeA=A.size();
        cout << "Number rows="<<sizeA[0]<<" Number of columns="<<sizeA[1]<<endl;
        cout << "Success:  testing tr procedure"<<endl;
        dmatrix AT=tr(A);
        cout << "Success:  result"<<endl<<AT<<endl;
        cout << "Success:  testing multiplication operator"<<endl
            << "First compute AT*A"<<endl;
        dmatrix prod;
        prod=AT*A;
        cout << "Result"<<endl;
        cout << prod<<endl;
        cout << "Testing scaling:   This should be previous (product) time 3"<<endl;
        dmatrix sprod;
        sprod=3.0*prod;
        cout << sprod<<endl;
        cout << "Testing operator - as 3AT*A-AT*A = 2AT*A"<<endl;
        dmatrix minus;
        minus=sprod-prod;
        cout << minus;
        cout << "Testing operator + as 3AT*A+AT*A=4AT*A"<<endl;
        dmatrix testadd;
        testadd=sprod+prod;
        cout << testadd<<endl;
        cout << "Testing some dvector components"<<endl;
        dvector x;
        x=Make_x();
        cout << "Result of Make_x procedure in this program"<<endl;
        cout<<x;
        cout << "Testing matrix vector multiply:  A*x"<<endl;
        dvector y;
        cout <<"A"<<endl<<A<<endl;
        cout << "x"<<endl<<x<<endl;
        cout << "Product y=Ax"<<endl;
        y=A*x;
        cout << y<<endl;
        exit(0);

    }catch(std::exception& stex)
    {
        cerr<<"Error:  something threw an exception.  This is the message"<<endl;
        cerr << stex.what()<<endl;
        exit(-1);
    }
}

