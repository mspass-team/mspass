#include <fstream>
//#include <boost/config.hpp>
//#include <boost/archive/text_oarchive.hpp>
//#include <boost/archive/text_iarchive.hpp>
#include "mspass/seismic/Seismogram.h"
#include "mspass/utility/AntelopePf.h"
using namespace std;
using namespace mspass;
/* This procedure probably should be added to mdlist.cc.  I writes the contents of a MetadataList to 
   stdout */
void print_mdlist(MetadataList& mdl)
{
    MetadataList::iterator mdlptr;
    for(mdlptr=mdl.begin();mdlptr!=mdl.end();++mdlptr)
    {
        cout << mdlptr->tag;
        switch(mdlptr->mdt)
        {
            case MDtype::Real:
                cout << " is real attribute";
                break;
            case MDtype::Integer:
                cout << " is integer attribute";
                break;
            case MDtype::Boolean:
                cout << " is boolean attribute";
                break;
            case MDtype::String:
                cout << " is string attribute";
                break;
            default:
                cout << " has unrecognized type";
        }
        cout << endl;
    }
}
int main(int argc, char **argv)
{
    string pfname("test_tcs");
    string amname="css3.0";
    //AntelopePf pf(pfname);
    ios::sync_with_stdio();
   try
   {
	int i,j;
	cout << "Testing simple constructors" << endl;
	Seismogram s1;
        cout << "Success"<<endl;
        cout << "Testing space allocating constructor"<<endl;
	Seismogram s2(100);
        cout << "Success"<<endl;
	// Initialize the contents of s2 and apply a rotation 
	// matrix
	cout << "trying rotation" << endl;
	for(i=0;i<3;++i)
		for(j=0;j<100;++j)
		{
			if(i==2) s2.u(i,j)=1.0;
			else
				s2.u(i,j)=0.0;
		}
        s2.live=true;
	SphericalCoordinate sc;
	sc.phi=0.0;
	sc.theta=M_PI_4;
	s2.rotate(sc);
	cout << "one sample of (0,0,1) rotated by phi=0, theta=45 deg:"
		<< s2.u(0,0) << ", "
		<< s2.u(1,0) << ", "
		<< s2.u(2,0) << endl;
        cout << "Testing operator[] for sample 0"<<endl;
        vector<double> tvec=s2[0];
        cout <<"Success - here is the result (should be identical to above) which was extracted raw"<<endl;
        cout << tvec[0]<<", "<<tvec[1]<<", "<<tvec[2]<<endl;
        cout << "Testing with the overloaded method for operator[double]"
            <<endl;
        double testtime(0.0);
        tvec=s2[testtime];
        cout << "Result - should be identical to previous"<<endl;
        cout << tvec[0]<<", "<<tvec[1]<<", "<<tvec[2]<<endl;
	cout << "Restoring (tests rotate_to_standard method)" << endl;
	s2.rotate_to_standard();
	cout << "One sample of restored (0,0,1): "
		<< s2.u(0,0) << ", "
		<< s2.u(1,0) << ", "
		<< s2.u(2,0) << endl;
        cout << "Trying theta=45 deg and phi=-45 deg "<<endl;
        sc.phi=-M_PI_4;
        double *nu1=SphericalToUnitVector(sc);
        s2.u(0,0)=nu1[0];  s2.u(1,0)=nu1[1];  s2.u(2,0)=nu1[2];
        delete [] nu1;
        s2.rotate(sc);
	cout << "one sample of unit vector pointing  phi=-45, theta=45 deg after rotation:  "
		<< s2.u(0,0) << ", "
		<< s2.u(1,0) << ", "
		<< s2.u(2,0) << endl;
        cout << "Should be approximately (0,0,1)"<<endl;
	cout << "Restoring to standard" << endl;
	s2.rotate_to_standard();
	cout << "One sample of restored phi-45 theta=45 vector:  "
		<< s2.u(0,0) << ", "
		<< s2.u(1,0) << ", "
		<< s2.u(2,0) << endl;

	Seismogram s3(100);
	for(i=0;i<3;++i)
		for(j=0;j<100;++j)
		{
			if(i==0) s3.u(i,j)=1.0;
			else
				s3.u(i,j)=0.0;
		}
        s3.live=true;
	// set some other vectors used in test below
	s3.u(0,1)=1.0;
	s3.u(1,1)=1.0;
	s3.u(2,1)=1.0;

	s3.u(0,2)=1.0;
	s3.u(1,2)=1.0;
	s3.u(2,2)=0.0;

	s3.u(0,3)=0.0;
	s3.u(1,3)=0.0;
	s3.u(2,3)=1.0;

	cout << "Trying unit vector rotation to (1,1,1) with (1,0,0) data"<<endl;
	double nu[3]={sqrt(3.0)/3.0,sqrt(3.0)/3.0,sqrt(3.0)/3.0};
	s3.rotate(nu);
	cout << "one sample of (1,0,0) rotated to (1,1,1)"
		<< s3.u(0,0) << ", "
		<< s3.u(1,0) << ", "
		<< s3.u(2,0) << endl;
	cout << "one sample of (1,1,1) rotated to (1,1,1)"
		<< s3.u(0,1) << ", "
		<< s3.u(1,1) << ", "
		<< s3.u(2,1) << endl;
	cout << "Restoring to standard" << endl;
	s3.rotate_to_standard();
	cout << "One sample of restored (1,0,0): "
		<< s3.u(0,0) << ", "
		<< s3.u(1,0) << ", "
		<< s3.u(2,0) << endl;
	cout << "One sample of restored (1,1,1): "
		<< s3.u(0,1) << ", "
		<< s3.u(1,1) << ", "
		<< s3.u(2,1) << endl;
	cout << "Trying spherical coordinate transformation to rotate phi=45 degrees,theta=0"
	<< endl;
	sc.phi=M_PI_4;
	sc.theta=0.0;
	s3.rotate(sc);
	cout << "one sample of rotated (1,0,0)"
		<< s3.u(0,0) << ", "
		<< s3.u(1,0) << ", "
		<< s3.u(2,0) << endl;
	cout << "one sample of rotated (1,1,0)"
		<< s3.u(0,2) << ", "
		<< s3.u(1,2) << ", "
		<< s3.u(2,2) << endl;
	cout << "one sample of rotated (0,0,1)"
		<< s3.u(0,3) << ", "
		<< s3.u(1,3) << ", "
		<< s3.u(2,3) << endl;
	s3.rotate_to_standard();
	cout << "Applying nonorthogonal transformation"<<endl;
	double a[3][3];
	a[0][0]=1.0;  a[0][1]=1.0;  a[0][2]=1.0;
	a[1][0]=-1.0;  a[1][1]=1.0;  a[1][2]=1.0;
	a[2][0]=0.0;  a[2][1]=-1.0;  a[2][2]=0.0;
	s3.transform(a);
	cout << "one sample of transformed (1,0,0)"
		<< s3.u(0,0) << ", "
		<< s3.u(1,0) << ", "
		<< s3.u(2,0) << endl;
	cout << "one sample of transformed (1,1,0)"
		<< s3.u(0,2) << ", "
		<< s3.u(1,2) << ", "
		<< s3.u(2,2) << endl;
	cout << "one sample of transformed (0,0,1)"
		<< s3.u(0,3) << ", "
		<< s3.u(1,3) << ", "
		<< s3.u(2,3) << endl;
	cout << "Testing back conversion to standard coordinates";
	s3.rotate_to_standard();
	cout << "One sample of restored (1,0,0): "
		<< s3.u(0,0) << ", "
		<< s3.u(1,0) << ", "
		<< s3.u(2,0) << endl;
	cout << "One sample of restored (1,1,1): "
		<< s3.u(0,1) << ", "
		<< s3.u(1,1) << ", "
		<< s3.u(2,1) << endl;
	cout << "One sample of restored (1,1,0): "
		<< s3.u(0,2) << ", "
		<< s3.u(1,2) << ", "
		<< s3.u(2,2) << endl;
	cout << "One sample of restored (0,0,1): "
		<< s3.u(0,3) << ", "
		<< s3.u(1,3) << ", "
		<< s3.u(2,3) << endl;

	cout << "Testing multiple, accumulated transformations" << endl;
	s3.rotate(nu);
	s3.transform(a);
	s3.rotate(sc);
	s3.rotate_to_standard();
	cout << "One sample of restored (1,0,0): "
		<< s3.u(0,0) << ", "
		<< s3.u(1,0) << ", "
		<< s3.u(2,0) << endl;
	cout << "One sample of restored (1,1,1): "
		<< s3.u(0,1) << ", "
		<< s3.u(1,1) << ", "
		<< s3.u(2,1) << endl;
	cout << "One sample of restored (1,1,0): "
		<< s3.u(0,2) << ", "
		<< s3.u(1,2) << ", "
		<< s3.u(2,2) << endl;
	cout << "One sample of restored (0,0,1): "
		<< s3.u(0,3) << ", "
		<< s3.u(1,3) << ", "
		<< s3.u(2,3) << endl;

        Seismogram s4(s3);
        cout << "Testing free_surface_transformation method"<<endl;
	SlownessVector uvec;
	uvec.ux=0.17085;  // cos(-20deg)/5.5
	uvec.uy=-0.062185; // sin(-20deg)/5.5
	//s4.free_surface_transformation(uvec,8.0,3.5); // test exception
	s4.free_surface_transformation(uvec,5.0,3.5);
        cout << "Completed"<<endl;
        /*
        ofstream ofs("testserial.dat");
        boost::archive::text_oarchive oa(ofs);
        oa << s4;
        ofs.close();
        cout << "Success"<<endl
            << "Attempting restore"
            <<endl;
        Seismogram s6;
        ifstream ifs("testserial.dat");
        boost::archive::text_iarchive ia(ifs);
        ia >> s6;
        ifs.close();
        cout << "Comparing before and after serialize" <<endl;
        dmatrix d4=s4.u;
        dmatrix d6=s6.u;
        dmatrix dio=d4-d6;
        double testsum;
        testsum=0.0;
        for(i=0;i<dio.rows();++i)
            for(j=0;j<dio.columns();++j)
                testsum += dio(i,j)*dio(i,j);
        cout << "Sum of squares of difference input - output"
            << testsum<<endl;;
            */

    }
    catch (MsPASSError&  serr)
    {
	serr.log_error();
    }
}
