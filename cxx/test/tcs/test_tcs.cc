#include <fstream>
#include <float.h>
#include <stdlib.h>  //not necessary on many systems but needed for random
#include "mspass/utility/MsPASSError.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/utility/AntelopePf.h"
#include "mspass/algorithms/algorithms.h"
#include "mspass/seismic/keywords.h"

using namespace std;
using namespace mspass::utility;
using namespace mspass::seismic;
using mspass::algorithms::ExtractComponent;
/* The idea of these functions was borrowed from python's is_close
functions.   These are for internal uses here and use overloading to handle
the different variations of entities to compare.  All assume doubles (64bit)
and use DBL_EPSILON */
const double IS_CLOSE_EPSILON(100.0*DBL_EPSILON); // N*eps*this is used for vector sums of diffs
bool is_close(const vector<double>& x, const vector<double>& y)
{
  if(x.size()!=y.size()) return false;
  vector<double>::const_iterator xptr,yptr;
  double sum;
  double nrmx(0.0),nrmy(0.0);
  for(xptr=x.begin(),yptr=y.begin(),sum=0.0;xptr!=x.end();++xptr,++yptr)
  {
    sum += (*xptr)-(*yptr);
    // use l1 norm
    nrmx=fabs(*xptr);
    nrmy=fabs(*yptr);
  }
  /* compute a scale factor as average of x and y nrm */
  double scale=(nrmx+nrmy)/(2.0*x.size());
  /* need if all are zeros */
  if(scale<IS_CLOSE_EPSILON) return true;
  double testval=(sum/x.size())/scale;
  if(fabs(testval) > IS_CLOSE_EPSILON)
    return false;
  else
    return true;
}
bool is_close(const dmatrix& x, const dmatrix& y)
{
  if(x.rows() != y.rows())
    return false;
  if(x.columns() != y.columns())
    return false;
  /* This is a large memory algorithm used to allow use of
  the vector version of this function above */
  vector<double> xtest,ytest;
  xtest.reserve(x.rows()*x.columns());
  ytest.reserve(y.rows()*y.columns());
  for(size_t i=0;i<x.rows();++i)
    for(size_t j=0;j<x.columns();++j)
    {
      xtest.push_back(x(i,j));
      ytest.push_back(y(i,j));
    }
  return is_close(xtest,ytest);
}
bool is_close(const dmatrix& x,const int column,const double *y)
{
  size_t i;
  vector<double> vx,vy;
  for(i=0;i<x.rows();++i)vx.push_back(x(i,column));
  for(i=0;i<x.rows();++i)vy.push_back(y[i]);
  return is_close(vx,vy);
}
void fill_random(CoreSeismogram& d)
{
  /* Fill the matrix with unscaled random numbers from stdlib random */
  int n;
  n=d.u.columns();
  size_t i,j;
  for(i=0;i<3;++i)
    for(j=0;j<n;++j) d.u(i,j)=(double)random();
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
	CoreSeismogram s1;
        cout << "Success"<<endl;
        cout << "Testing space allocating constructor"<<endl;
	CoreSeismogram s2(100);
        cout << "Success"<<endl;
	// Initialize the contents of s2 and apply a rotation
	// matrix
	cout << "trying rotation" << endl;
  	s2.set_live();
  	s2.set_npts(100);
  	s2.set_t0(0.0);
	/* Has to be after set_npts - api change made that clear
	contents to 0s*/
	for(j=0;j<100;++j) s2.u(2,j)=1.0;
	/* Added test March 2021 for ExtractComponent setting hang
	and vang properly.  First this test for cardinal */


	cout << "Testing ExtractComponent helper"<<endl;
	dmatrix tmex;
	tmex=s2.get_transformation_matrix();
	cout << "Tranformation matrix"<<endl<<tmex<<endl;
  for(i=0;i<3;++i)
	{
	  TimeSeries ts;
	  ts=ExtractComponent(s2,i);
	  cout << "component="<<i<<" hang="<<ts.get_double(SEISMICMD_hang)
	     << " vang="<<ts.get_double(SEISMICMD_vang)<<endl;
	}
	cout << "one sample prior to rotation:"<<endl
		<< s2.u(0,0) << ", "
		<< s2.u(1,0) << ", "
		<< s2.u(2,0) << endl;
  /* This is the analytic result for the transformation we are about to
  compute with rotate*/
  dmatrix mtest(3,3);
  mtest.zero();
  mtest(0,0)=1.0;
  mtest(1,1)=1.0/sqrt(2.0);
  mtest(2,1)=1.0/sqrt(2.0);
  mtest(1,2)= -1.0/sqrt(2.0);
  mtest(2,2)=1.0/sqrt(2.0);
  /* Needed to test this rotate method - kind of weird and easily confused.
  see doxygen description */
  SphericalCoordinate sc;
	sc.phi=M_PI_2;
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
  vector<double> vtest;
  vtest.push_back(0.0);
  vtest.push_back(-1.0/sqrt(2.0));
  vtest.push_back(1.0/sqrt(2.0));
  cout << "Result - should be identical to previous"<<endl;
  cout << tvec[0]<<", "<<tvec[1]<<", "<<tvec[2]<<endl;
	cout << "Testing ExtractComponent helper on rotated data (not cardinal)"<<endl;
	tmex=s2.get_transformation_matrix();
	cout << "Tranformation matrix"<<endl<<tmex<<endl;
  assert(is_close(tmex,mtest));
  assert(is_close(tvec,vtest));
  cout << "Testing ExtractComponent setting hang and vang"<<endl;
  vector<double> testhang, testvang,hang0,vang0;
  for(i=0;i<3;++i)
  {
    switch(i)
    {
      case 0:
        hang0.push_back(90.0);
        vang0.push_back(90.0);
        break;
      case 1:
        hang0.push_back(0.0);
        vang0.push_back(135.0);
        break;
      case 2:
        hang0.push_back(0.0);
        vang0.push_back(45.0);
    }
  }
	for(i=0;i<3;++i)
	{
	  TimeSeries ts;
	  ts=ExtractComponent(s2,i);
	  cout << "component="<<i<<" hang="<<ts.get_double(SEISMICMD_hang)
	     << " vang="<<ts.get_double(SEISMICMD_vang)<<endl;
    testhang.push_back(ts.get_double(SEISMICMD_hang));
    testvang.push_back(ts.get_double(SEISMICMD_vang));
	}
  assert(is_close(hang0,testhang));
  assert(is_close(vang0,testvang));
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
  cout << "Testing rigorous rotate followed by rotate_to_standard of random matrix";
  fill_random(s2);
  dmatrix s2u0(s2.u);
  s2.rotate(sc);
  s2.rotate_to_standard();
  assert(is_close(s2.u,s2u0));
  cout <<"Success"<<endl;
	CoreSeismogram s3(100);
  	s3.set_npts(100);
	for(i=0;i<3;++i)
		for(j=0;j<100;++j)
		{
			if(i==0) s3.u(i,j)=1.0;
			else
				s3.u(i,j)=0.0;
		}
  	s3.set_live();
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
  double vt3[3]={0.0,0.0,sqrt(3.0)};
  assert(is_close(s3.u,1,vt3));
	cout << "Restoring to standard" << endl;
	s3.rotate_to_standard();
	cout << "One sample of restored (1,0,0): "
		<< s3.u(0,0) << ", "
		<< s3.u(1,0) << ", "
		<< s3.u(2,0) << endl;
  vt3[0]=1.0; vt3[1]=0.0;  vt3[2]=0.0;
  assert(is_close(s3.u,0,vt3));
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
  vt3[0]=1.0; vt3[1]=-1.0;  vt3[2]=0.0;
  assert(is_close(s3.u,0,vt3));
  cout << "One sample of transformed (1,1,1): "
		<< s3.u(0,1) << ", "
		<< s3.u(1,1) << ", "
		<< s3.u(2,1) << endl;
  vt3[0]=3.0; vt3[1]=1.0;  vt3[2]=-1.0;
  assert(is_close(s3.u,1,vt3));
	cout << "one sample of transformed (1,1,0):  "
		<< s3.u(0,2) << ", "
		<< s3.u(1,2) << ", "
		<< s3.u(2,2) << endl;
  vt3[0]=2.0; vt3[1]=0.0;  vt3[2]=-1.0;
  assert(is_close(s3.u,2,vt3));
	cout << "one sample of transformed (0,0,1):  "
		<< s3.u(0,3) << ", "
		<< s3.u(1,3) << ", "
		<< s3.u(2,3) << endl;
  vt3[0]=1.0; vt3[1]=1.0;  vt3[2]=0.0;
  assert(is_close(s3.u,3,vt3));
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
  vt3[0]=1.0; vt3[1]=0.0;  vt3[2]=0.0;
  assert(is_close(s3.u,0,vt3));
  vt3[0]=1.0; vt3[1]=1.0;  vt3[2]=1.0;
  assert(is_close(s3.u,1,vt3));
  vt3[0]=1.0; vt3[1]=1.0;  vt3[2]=0.0;
  assert(is_close(s3.u,2,vt3));
  vt3[0]=0.0; vt3[1]=0.0;  vt3[2]=1.0;
  assert(is_close(s3.u,3,vt3));
	cout << "Testing multiple, accumulated transformations" << endl;
	s3.rotate(-M_PI_4);
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

  /* We don't do assert tests on all of the above because
  not all yield analytic results computable as exact  values.*/
  vt3[0]=sqrt(2.0); vt3[1]=0.0;  vt3[2]=-1.0/sqrt(2.0);
  assert(is_close(s3.u,0,vt3));
  vt3[0]=sqrt(2.0); vt3[1]=sqrt(2.0);  vt3[2]=-sqrt(2.0);
  assert(is_close(s3.u,2,vt3));
  vt3[0]=1.0; vt3[1]=1.0;  vt3[2]=0.0;
  assert(is_close(s3.u,3,vt3));
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
  /* We don't do assert tests on all of the above because
  not all yield analytic results computable as exact  values.*/
  vt3[0]=1.0; vt3[1]=0.0;  vt3[2]=0.0;
  assert(is_close(s3.u,0,vt3));
  vt3[0]=1.0; vt3[1]=0.0;  vt3[2]=0.0;
  assert(is_close(s3.u,0,vt3));
  vt3[0]=1.0; vt3[1]=0.0;  vt3[2]=0.0;
  assert(is_close(s3.u,0,vt3));
	/* We put this to metadata for verification below */
	s3.put_string("foo","bar");
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
        	exit(-1);
	}
	cout << "Testing Seismogram copy constructor"<<endl;
	Seismogram s6(s5);
	if(s6.live())
		cout << "Result is still marked live"<<endl;
	else
	{
		cout << "Result is marked dead - error"<<endl;
        	exit(-1);
	}
	/* Test BasicTimeSeries was copied properly and verify
	expected set metadata were copied */
	if( (s5.t0()!=s6.t0()) || (s5.npts()!=s6.npts()) || (s5.dt()!=s6.dt())
		|| (s5.timetype() != s6.timetype()) )
	{
		cout << "Error - copy constructor for Seismogram did not "
			<< "cleanly copy BasicTimeSeries"<<endl
			<< "Bug fix is required"<<endl;
		exit(-1);
	}
	try {
		string strtest;
		strtest=s5.get_string("foo");
		if(strtest!="bar")
		{
			cout << "Error - test metadata in Seismogram was garbled"<<endl;
			exit(-1);
		}
	}catch( MsPASSError& merr)
	{
		cout << merr.what()<<endl;
		cout << "Partial copy constructor for Seismogram from CoreSeismogram has a problem"<<endl;
		exit(-1);
	}
	try {
		string strtest;
		strtest=s6.get_string("foo");
		if(strtest!="bar")
		{
			cout << "Error - test metadata in Seismogram was garbled in Seismogram copy constructor"<<endl;
			exit(-1);
		}
	}catch( MsPASSError& merr)
	{
		cout << merr.what();
		cout << "Seismogram copy constructor has a problem"<<endl;
		exit(-1);
	}
	cout << "Copy constructor for Seismogram passed all tests"<<endl;
    }
    catch (MsPASSError&  serr)
    {
	serr.log_error();
        exit(-1);
    }
}
