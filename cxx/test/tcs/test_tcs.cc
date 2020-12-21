#include <fstream>
//#include <boost/config.hpp>
//#include <boost/archive/text_oarchive.hpp>
//#include <boost/archive/text_iarchive.hpp>
#include "mspass/seismic/CoreSeismogram.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/utility/AntelopePf.h"
using namespace std;
using namespace mspass::utility;
using namespace mspass::seismic;
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
	CoreSeismogram s1;
        cout << "Success"<<endl;
        cout << "Testing space allocating constructor"<<endl;
	CoreSeismogram s2(100);
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
  s2.set_live();
  s2.set_npts(100);
  s2.set_t0(0.0);
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

	CoreSeismogram s3(100);
	for(i=0;i<3;++i)
		for(j=0;j<100;++j)
		{
			if(i==0) s3.u(i,j)=1.0;
			else
				s3.u(i,j)=0.0;
		}
  s3.set_live();
  s3.set_npts(100);
  s3.set_t0(0.0);
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
        cout << "Transformation matrix created by this method:"<<endl;
        dmatrix tm=s3.get_transformation_matrix();
        cout << tm<<endl;
	cout << "one sample of (1,0,0) rotated to (1,1,1)"
		<< s3.u(0,0) << ", "
		<< s3.u(1,0) << ", "
		<< s3.u(2,0) << endl;
	cout << "one sample of (1,1,1) rotated to (1,1,1)"
		<< s3.u(0,1) << ", "
		<< s3.u(1,1) << ", "
		<< s3.u(2,1) << endl;
        cout << "Should be (0,0,sqrt(3))"<<endl;
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
        cout << "Transformation matrix created by this method:"<<endl;
        tm=s3.get_transformation_matrix();
        cout << tm<<endl;
	cout << "one sample of transformed (1,0,0)"
		<< s3.u(0,0) << ", "
		<< s3.u(1,0) << ", "
		<< s3.u(2,0) << endl;
	cout << "One sample of transformed (1,1,1): "
		<< s3.u(0,1) << ", "
		<< s3.u(1,1) << ", "
		<< s3.u(2,1) << endl;
	cout << "one sample of transformed (1,1,0)"
		<< s3.u(0,2) << ", "
		<< s3.u(1,2) << ", "
		<< s3.u(2,2) << endl;
	cout << "one sample of transformed (0,0,1)"
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
	cout << "one sample of transformed (1,0,0):  "
		<< s3.u(0,0) << ", "
		<< s3.u(1,0) << ", "
		<< s3.u(2,0) << endl;
	cout << "One sample of transformed (1,1,1): "
		<< s3.u(0,1) << ", "
		<< s3.u(1,1) << ", "
		<< s3.u(2,1) << endl;
	cout << "one sample of transformed (1,1,0):  "
		<< s3.u(0,2) << ", "
		<< s3.u(1,2) << ", "
		<< s3.u(2,2) << endl;
	cout << "one sample of transformed (0,0,1):  "
		<< s3.u(0,3) << ", "
		<< s3.u(1,3) << ", "
		<< s3.u(2,3) << endl;
	cout << "Testing back conversion to standard coordinates"<<endl;
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
	s3.rotate(M_PI_4);
	s3.transform(a);
        cout << "Accumulated transformation matrix from double transformation"
            <<endl;
        tm=s3.get_transformation_matrix();
        cout << tm;
	cout << "one sample of transformed (1,0,0):  "
		<< s3.u(0,0) << ", "
		<< s3.u(1,0) << ", "
		<< s3.u(2,0) << endl;
	cout << "One sample of transformed (1,1,1): "
		<< s3.u(0,1) << ", "
		<< s3.u(1,1) << ", "
		<< s3.u(2,1) << endl;
	cout << "one sample of transformed (1,1,0):  "
		<< s3.u(0,2) << ", "
		<< s3.u(1,2) << ", "
		<< s3.u(2,2) << endl;
	cout << "one sample of transformed (0,0,1):  "
		<< s3.u(0,3) << ", "
		<< s3.u(1,3) << ", "
		<< s3.u(2,3) << endl;
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

        CoreSeismogram s4(s3);
        cout << "Testing free_surface_transformation method"<<endl;
	SlownessVector uvec;
	uvec.ux=0.17085;  // cos(-20deg)/5.5
	uvec.uy=-0.062185; // sin(-20deg)/5.5
	//s4.free_surface_transformation(uvec,8.0,3.5); // test exception
	s4.free_surface_transformation(uvec,5.0,3.5);
        cout << "Completed"<<endl;
        cout << "Computed transformation matrix:"<<endl;
        tm=s4.get_transformation_matrix();
        cout << tm<<endl;
	///
	cout << "Testing Seismogram constructor from CoreSeismogram"<<endl;
	Seismogram s5(s4);
	cout << "Transformation matrix - should be same as previous"<<endl;
	tm = s5.get_transformation_matrix();
	cout << tm<<endl;
	if(s5.live())
		cout << "Result is still marked live"<<endl;
	else
	{
		cout << "Result is marked dead - error"<<endl;
        	exit(0);
	}
	cout << "Testing Seismogram copy constructor"<<endl;
	Seismogram s6(s5);
	if(s6.live())
		cout << "Result is still marked live"<<endl;
	else
	{
		cout << "Result is marked dead - error"<<endl;
        	exit(0);
	}
    }
    catch (MsPASSError&  serr)
    {
	serr.log_error();
        exit(-1);
    }
}
