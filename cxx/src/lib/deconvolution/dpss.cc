//#include "perf.h"
#include <stdlib.h>   
#include "mspass/deconvolution/dpss.h"
using namespace std;
namespace mspass{
void compute_energy_concentrations(double *h, int n, double NW, double *lambda, int nseq) {

    //kernel
    double *kern= new double[n];

    kern[0]=2.0*NW/n;
    for (int ii=1; ii<n; ii++) {
        kern[ii]=sin(2.0*PI*NW/n*ii)/(PI*ii);
    }

    double v;
    for (int ii=0; ii<nseq; ii++) {

        lambda[ii]=0;
        for (int jj=0; jj<n; jj++) {
            v=0;
            for (int kk=0; kk<n; kk++) {
                v=v+kern[(int)abs((int)(jj-kk))]*h[(int)(n*ii+kk)];
            }
            lambda[ii]+=v*h[n*ii+jj];
        }
    }

    return;

}

void eig_iit(int n, double *D, double *E, int il, int iu, double *eig_val, double *eig_vec, int vec_length) {

    //output params
    char range='I', order='B', safe='S';    //index range of eigenvalues, using block splitting and safe tolerance
    double vl,vu;                           //dummy variables
    int ldz=vec_length;                     //size of z array
    double abstol=2*dlamch_(&safe);         //safe minimum absolute tolerance
    int nseq=iu-il+1;                 //number of returned eigenvalues/vectors

    //intermediaries
    int m, nsplit;
    int *IBLOCK=new int[n];
    int *ISPLIT=new int[n];
    int *IWORK=new int[3*n];
    double *WORK = new double[5*n];               //dstebz needs 4n, but dstein needs 5n

    //outputs
    int *IFAIL=new int[nseq];
    int info;

    //calculate eigenvalues
    dstebz_(&range, &order, &n, &vl, &vu, &il, &iu, &abstol, D, E, &m, &nsplit, eig_val, IBLOCK, ISPLIT, WORK, IWORK, &info);
    if (info == 0) {
        dstein_(&n, D, E, &m, eig_val, IBLOCK, ISPLIT, eig_vec, &ldz, WORK, IWORK, IFAIL, &info );
    }
    //cleanup
    delete [] IBLOCK;
    delete [] ISPLIT;
    delete [] IWORK;
    delete [] WORK;
    delete [] IFAIL;

    if (info!=0) {
        throw LAPACK_ERROR("DSTEIN failed");
    }
}


//normalizes a vector h
void normalize_vec(double *h, int n) {

    double norm=0;
    for ( int ii=0; ii<n; ii++)
        norm=norm+h[ii]*h[ii];
    norm=sqrt(norm);

    for ( int ii=0; ii<n; ii++)
        h[ii]=h[ii]/norm;

}

//polarizes the sequences
void polarize_dpss(double *h, int n, int iseq) {

    double sum=0;

    //force positive mean for symmetric sequences, positive first lobe for antisymmetric
    int odd=(iseq%2);
    for (int ii=0; ii<n/2; ii++)
        sum+=(odd*(n-1-2*ii)+1-odd)*h[ii];

    //if mean is negative, flip vector
    if (sum<0) {
        for (int ii=0; ii<n; ii++)
            h[ii]=-h[ii];
    }
}

//Reduces the problem using simple even/odd splitting (exploiting double symmetry)
void dpss_calc(int n, double NW, int seql, int sequ, double *h) {

    int nseq=sequ-seql+1;
    bool is_n_even=(n%2==0);

    //sizes of even/odd problems
    int n_even=ceil(n/2.0);
    int n_odd=floor(n/2.0);
    int seql_even=ceil(seql/2.0);
    int seql_odd=floor(seql/2.0);
    int sequ_even=floor(sequ/2.0);
    int sequ_odd=floor((sequ-1)/2.0);
    int nseq_even=sequ_even-seql_even+1;
    int nseq_odd=sequ_odd-seql_odd+1;

    //use same vectors for even/odd problem
    double *D=new double[n_even];
    double *E=new double[n_even];
    double *eig_val=new double[nseq];

    //fill tridiagonal matrix
    double cc=cos(2.0*PI*NW/n)/4.0;
    for (int ii=0; ii < n_even; ii++) {
        D[ii]=(n-1-2.0*ii)*(n-1-2.0*ii)*cc;
        E[ii]=(ii+1)*(n-ii-1)/2.0;
    }

    //central values for splitting
    double dc=D[n_even-1], ec=E[n_even-1];

    //EVEN PROBLEM
    if (is_n_even)
        D[n_even-1]=dc+ec;
    else
        E[n_odd-1]=sqrt(2)*E[n_odd-1];

    int start_odd=0,start_even=0;
    if (sequ%2==0)
        start_odd=n;
    else
        start_even=n;

    //calculate eigenvectors
    if (nseq_even>0)
        eig_iit(n_even,D,E,n_even-sequ_even,n_even-seql_even,eig_val,&h[start_even],2*n);

    //ODD PROBLEM
    D[n_even-1]=dc-ec;
    if (nseq_odd>0)
        eig_iit(n_odd,D,E,n_odd-sequ_odd,n_odd-seql_odd,&eig_val[nseq_even],&h[start_odd],2*n);

    //reverse eigenvector order
    double tmp;
    for (int ii=0; ii<floor(nseq/2); ii++) {
        for (int jj=0; jj < n_even; jj++) {
            tmp=h[(nseq-1-ii)*n+jj];
            h[(nseq-1-ii)*n+jj]=h[ii*n+jj];
            h[ii*n+jj]=tmp;
        }
    }

    //construct full vectors
    int c=1;
    if (seql%2==1) c=-1;
    for (int ii=0; ii<nseq; ii++) {
        for (int jj=0; jj<n_odd; jj++) {
            h[(ii+1)*n-1-jj]=c*h[ii*n+jj];
        }
        //correct centre value
        if (!is_n_even) {
            if ((ii+seql)%2==1)
                h[ii*n+n_odd]=0;
            else
                h[ii*n+n_odd]=sqrt(2)*h[ii*n+n_odd];
        }
        c=-c;
    }

    //normalize and polarize the eigenvectors
    for (int ii=0; ii<nseq; ii++) {
        normalize_vec(&h[n*ii],n);
        polarize_dpss(&h[n*ii],n,seql+ii);
    }

    //cleanup
    delete [] D;
    delete [] E;
    delete [] eig_val;

}

ERR::ERR(const char *errmsg) {
    strncpy(msg,errmsg,30);
}
void ERR::getmsg(char *errmsg) {
    strncpy(errmsg,this->msg,30);
}
const char *ERR::getmsg() {
    return msg;
}

LAPACK_ERROR::LAPACK_ERROR(const char *errmsg) : ERR(errmsg) {};

}  //end namespace
