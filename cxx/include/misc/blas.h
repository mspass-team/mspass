#ifndef __BLAS_H
#define __BLAS_H

#include "FC.h"

extern "C" {
double ddot(const int& n,const double *x,const int& incx,
        const double *y,const int& incy);
double dscal(const int& n, const double& a, const double *x, const int& incx);
double daxpy(const int &n, const double& a, const double *x,const int& incx,
        const double *y,const int& incy);
double dcopy(const int &n, const double *x,const int& incx,
        const double *y,const int& incy);
double dnrm2(const int &n, const double *x,const int& incx);
void dgetrf(int&,int&,double*,int&,int*,int&);
void dgetri(int&,double*,int&,int*,double*,int&,int&);
double dlamch(const char *cmach);
int dstebz(char *range, char *order, int *n, double 
	*vl, double *vu, int *il, int *iu, double *abstol, 
	double *d__, double *e, int *m, int *nsplit, 
	double *w, int *iblock, int *isplit, double *work, 
	int *iwork, int *info);
int dstein(int *n, double *d__, double *e, 
	int *m, double *w, int *iblock, int *isplit, 
	double *z__, int *ldz, double *work, int *iwork, 
	int *ifail, int *info);

};

#endif
