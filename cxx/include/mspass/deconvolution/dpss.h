#ifndef __DPSS_H__
#define __DPSS_H__
#include <cmath>
#include <string.h>
#define PI 3.141592653589793238463
namespace mspass{

//Error classes for LAPACK, and a general error
class ERR {
public:
    ERR() {};
    ERR(const char *msg);
    void getmsg(char *errmsg);
    const char *getmsg();
protected:
    char msg[30];
};

class LAPACK_ERROR : public ERR {
public:
    LAPACK_ERROR() {};
    LAPACK_ERROR(const char *errmsg);
};

void compute_energy_concentrations(double *h, int n, double NW, double *lambda, int nseq);

void eig_iit(int n, double *D, double *E, int il, int iu, double *eig_val, double *eig_vec, int vec_length);

//normalizes a vector h
void normalize_vec(double *h, int n);

//polarizes the sequences
void polarize_dpss(double *h, int n, int iseq);

//Reduces the problem using simple even/odd splitting (exploiting double symmetry)
void dpss_calc(int n, double NW, int seql, int sequ, double *h);
}
#endif
