//  Copyright 2016, INSEAD
//  by V. Kapartzianis 
//  Dual licensed under the MIT or GPL Version 2 licenses.

#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::plugins(openmp)]]

// [[Rcpp::export]]
NumericVector row_weights(NumericMatrix x, NumericVector weight) {
  int nX = x.ncol();
  int nY = x.nrow();
  NumericVector v = no_init(nY);
  
  //#pragma omp parallel for schedule(static)
  for (int i=0; i < nY; i++) {
    NumericMatrix::Row row = x(i, _);

    double w = 0;
    for (int j=0; j < nX; j++) {
      if(row[j]!=0 && !R_IsNA(row[j])) {
        w += weight[j];
      }
    }

    double o = 0;
    if (w!=0) {
      for(int j=0; j < nX; j++) {
        if(row[j]!=0 && !R_IsNA(row[j])) {
          o += row[j]*weight[j] / w;
        }
      }
    }
    
    v[i] = o;
  }
  
  v.attr("names") = rownames(x);
  return v;
}


// [[Rcpp::export]]
NumericMatrix calendar_table_helper1(NumericMatrix x, NumericVector Row_Date_number, NumericVector Event_Date_number, NumericVector hitnow) {
//NOTE: it would be much faster to update the matrix without cloning it, but
//that could lead to unexpected bugs, as R wouldn't know the matrix was updated.
  NumericMatrix ret = Rcpp::clone(x);
  int nX = ret.ncol();
  int nY = ret.nrow();
  
  //#pragma omp parallel for schedule(static)
  for (int col=0; col < nX; col++) {
    for (int row=0; row < nY; row++) {
      if (Row_Date_number[row] <= Event_Date_number[col] || Row_Date_number[row] > hitnow[col]) {
        ret(row, col) = 0;
      }
    }
  }
  return ret;
}
// [[Rcpp::export]]
NumericMatrix calendar_table_helper2(NumericMatrix x, NumericVector Row_Date_number, NumericVector Event_Date_number, NumericVector hitnow) {
//NOTE: it would be much faster to update the matrix without cloning it, but
//that could lead to unexpected bugs, as R wouldn't know the matrix was updated.
  NumericMatrix ret = Rcpp::clone(x);
  int nX = ret.ncol();
  int nY = ret.nrow();
  
  //#pragma omp parallel for schedule(static)
  for (int col=0; col < nX; col++) {
    for (int row=0; row < nY; row++) {
      if (Row_Date_number[row] >= Event_Date_number[col] || Row_Date_number[row] < hitnow[col]) {
        ret(row, col) = 0;
      }
    }
  }
  return ret;
}

/*** R
row_weights.R <- function(x, weight) {
  apply(x, 1, function(row) {
    n = sum((row!=0)*weight, na.rm=TRUE)
    if (n!=0) sum(row*weight / n, na.rm=TRUE) else 0
  })
}
*/
