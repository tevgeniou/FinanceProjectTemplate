#  Copyright 2013, Satrapade
#  by T. Evgeniou, V. Kapartzianis, N. Nassuphis and D. Spinellis
#  Dual licensed under the MIT or GPL Version 2 licenses.
#  11/2015 functions car_table and calendar_table added by T. Evgeniou and E. Junqu?? de Fortuny

# Required R libraries (need to be installed - it can take a few minutes the first time you run the project)

if (ifelse(!exists("run_shiny_tool"), T, run_shiny_tool == 0)) { # When deploying a shiny app on the shiny server we should not load all R libraries
  # installs all necessary libraries from CRAN
  get_libraries <- function(filenames_list) { 
    lapply(filenames_list,function(thelibrary){    
      if (do.call(require,list(thelibrary)) == FALSE) 
        do.call(install.packages,list(thelibrary)) 
      do.call(library,list(thelibrary))
    })
  }
  libraries_used=c("stringr","gtools","timeDate","data.table","psych","RcppArmadillo","Hmisc","DescTools")
  
  get_libraries(libraries_used)
  Rcpp::sourceCpp('lib_helpers.cpp', embeddedR=FALSE)
} else {
  lib_helpers_path <- try(suppressWarnings(dirname(sys.frame(1)$ofile)), silent=TRUE)
  if (class(lib_helpers_path) == "try-error") lib_helpers_path <- "."
  if (file.exists("lib_helpers.cpp")) lib_helpers_path <- "."
  Rcpp::sourceCpp(file.path(lib_helpers_path,'lib_helpers.cpp'), embeddedR=FALSE)
}

options(stringsAsFactors=FALSE)


non_na_mean <- function(x) { mean(x[!is.na(x)]) }

remove_initialization_time <- function(x,min_date=NULL) { # Note: Added also min_date as an argument!! THis way we remove data from any given date!
  if (!is.null(min_date)){
    res = x[as.Date(names(x)) >= as.Date(min_date)]
  } else {
    if (x[1] != 0){
      res = x
    } else {
      if (sum(x!=0) != 0){
        tmp <- names(x)
        ending <- -head(which(diff(x)!=0),1)
        res = tail(x, ending)
        names(res) <- tail(tmp, ending)
      }
    }
  }
  res
}

yeardays<-function(x){
  fd<-as.integer(as.Date(head(names(x),1),format="%Y-%m-%d"))
  ld<-as.integer(as.Date(tail(names(x),1),format="%Y-%m-%d"))
  365.25*length(x)/(ld-fd)
}

vol_pa<-function(x,exclude_zero=(x!=0), holidays = timeDate::holidayNYSE()) {
  if(is.null(names(x))){ good_days <- TRUE }else { good_days <- timeDate::isBizday(timeDate::as.timeDate(names(x)), holidays, wday = 1:5) }
  x1<-drop(x[exclude_zero & good_days])
  sqrt(yeardays(x1))*sd(x1)
}

sharpe<-function(x,exclude_zero=(x!=0), holidays = timeDate::holidayNYSE() ) {
  if(is.null(names(x))){ good_days <- TRUE }else { good_days <- timeDate::isBizday(timeDate::as.timeDate(names(x)), holidays, wday = 1:5) }
  x1<-drop(x[exclude_zero & good_days])
  round(yeardays(x1)*mean(x1)/vol_pa(x1),digits=2)   # annualized
}

drawdown<-function(x){
  scaling = ifelse(max(abs(x)) < 20, 100, 1)
  round(scaling*max(cummax(cumsum(x))-cumsum(x)),digits=2)
}

pnl_stats<-function(x, show_tr = FALSE, show_gr = FALSE){
  if(class(x)=="matrix")if(ncol(x)>1)x<-x[,1]
  scaling = ifelse(max(abs(x)) < 20, 100, 1) # Depending on whether the returns are 0 to 1 or 0 to 100%
  ret<-c(Ret=round(scaling*yeardays(x)*mean(x),digits=1),Vol=round(scaling*vol_pa(x),digits=1),Sharpe=round(sharpe(x),2),DD=round(drawdown(x),1))
  ret
}

pnl_plot<-function(x,...){
  pargs<-as.list(match.call(expand.dots=TRUE))
  if(!"ylab" %in% names(pargs)) ylab<-deparse(substitute(x)) else ylab<-pargs$ylab
  if(!"main" %in% names(pargs)) main<-paste(names(pnl_stats(x)),pnl_stats(x),sep=":",collapse=" ") else main<-pargs$main
  plot_arguments<-c(list(x=cumsum(x*100),type="l",ylab=ylab,cex.main = 0.9, main=main,axes=FALSE),pargs[setdiff(names(pargs),c("","x","ylab","main"))])
  do.call(plot,plot_arguments)
  if(!is.null(names(x))){
    axis(1,at=seq(1,length(x),length.out=5),labels=names(x)[seq(1,length(x),length.out=5)])
    axis(2)
  } else { axis(1); axis(2)}
}

pnl_matrix<-function(perf, digits = 2){
  
  month_map<-c("01"="Jan","02"="Feb","03"="Mar","04"="Apr","05"="May","06"="Jun","07"="Jul","08"="Aug","09"="Sep","10"="Oct","11"="Nov","12"="Dec")
  perf_dates<-structure(do.call(rbind,strsplit(names(perf),"-")),dimnames=list(names(perf),c("Year","Month","Day")))
  perf_dates[,"Month"]<-month_map[perf_dates[,"Month"]]
  perf_years  <- sort(unique(perf_dates[,"Year"]))
  perf_months <- month_map
  res<-structure(
    outer(perf_years,perf_months,function(i_vec,j_vec)mapply(function(i,j){
      perf_ndx <- perf_dates[,"Year"]==i & perf_dates[,"Month"]==j
      if(sum(perf_ndx)==0)return(NA)
      prod(perf[perf_ndx]+1)-1
    },i_vec,j_vec)),
    dimnames=list(perf_years,perf_months)
  )
  round(cbind(res,Year=apply(res,1,function(r)prod(r[!is.na(r)]+1)-1))*100,digits=2)
}

###

# if number of non-zeroes of x is less than n, return 0, else return the mean of the non-zero entries 
non_zero_mean<-function(x,n=1)ifelse(sum(scrub(x)!=0)<n,0,mean(scrub(x)[scrub(x)!=0]))
# moving average of previous n elements : 0 for first n-1 elements
ma<-function(x,n,f=identity){res<-as.numeric(filter(f(x),rep(1/n,n),method="convolution",sides=1,circular=FALSE)); ifelse(is.na(res),0,res)}
# moving sum of previous n elements : 0 for first n-1 elements
ms<-function(x,n,f=identity){res<-as.numeric(filter(f(x),rep(1,n),method="convolution",sides=1,circular=FALSE)); ifelse(is.na(res),0,res)}
# shift forward if n +ve , backward if n -ve.
moving_average<-function(n)function(x)ma(x,n)
moving_sum<-function(n)function(x)ms(x,n)

shift<-function(a,n=1,filler=0){
  x<-switch(class(a),matrix=a,matrix(a,ncol=1,dimnames=list(names(a),NULL)))
  if(n==0)return(x)
  if(n>0){
    rbind(matrix(filler,ncol=ncol(x),nrow=n),head(x,-n)) 
  } else {
    rbind(tail(x,n),matrix(filler,ncol=ncol(x),nrow=abs(n)))
  }
}
# replace non-finite elements in x with zeroes
scrub<-function(x){
  if(length(x)==0)return(0)
  x[which(!is.finite(x))]<-0
  x
}

# Fill NAs with earlier values
fill_NA_previous <- function(x, lastfill = F){
  if (sum(!is.na(x)) == 0)
    return(x)
  non_na = which(!is.na(x))
  if (length(non_na) == 1)
    return(x)
  if (!(1 %in% non_na))  non_na = c(1,non_na)
  x = c(unlist(lapply(1:(length(non_na)-1), function(i) rep(x[non_na[i]],non_na[i+1] - non_na[i]))), x[tail(non_na,1):length(x)])
  if (lastfill)
    x[tail(non_na,1):length(x)] <- x[tail(non_na,1)]
  x
}

######

# roll function over multiple matrix rows, in parallel. mapply for matrices.
multiroll_fun<-function(fun,w,...,MoreArgs=NULL,verbose=identity){
  data<-list(...)
  if(length(data)==0)stop("multiroll: data required")
  if(length(data)!=length(formals(fun)))stop("multiroll: fun arg count differs from data length")
  if(!all(unlist(lapply(data,class))=="matrix"))stop("multiroll: only matrix")
  if( length(unique(unlist(lapply(data,nrow))))!=1 )stop("multiroll: all matrices must have same rows")
  p<-nrow(data[[1]])
  ndx<-t(matrix(c(0,w-1),nrow=2,ncol=p-w+1)+t(matrix(1:(p-w+1),ncol=2,nrow=p-w+1)))
  res<-t(simplify2array(apply(ndx,1,function(r){
    window_res<-do.call(fun,c(lapply(data,function(m)m[r[1]:r[2],,drop=FALSE]),MoreArgs))
    if(class(verbose)=="function")cat(r[2]," ",verbose(window_res),"\n")
    window_res
  })))
  if(class(res)=="matrix")rownames(res)<-rownames(data[[1]])[ndx[,2]] else names(res)<-rownames(data[[1]])[ndx[,2]]
  res
}

# apply function to matrix, matrix result
row_apply<-function(m,f,...){
  if(class(m)!="matrix")return(NULL)
  if(!any(class(f)%in%c("function","standardGeneric")))return(NULL)
  mcol<-ncol(m)
  res<-t(apply(m,1,function(r){
    row_res<-drop(unlist(f(r,...)))
    if(length(row_res)==1)return(rep(row_res,mcol))
    if(length(row_res)==mcol)return(row_res)
    return(rep(0,length(r)))
  }))
  dimnames(res)<-dimnames(m)
  res
}

# apply function to matrix, matrix result
col_apply<-function(m,f,...){
  if(class(m)!="matrix")return(NULL)
  if(!any(class(f)%in%c("function","standardGeneric")))return(NULL)
  mrow<-nrow(m)
  res<-apply(m,2,function(r){
    row_res<-drop(unlist(f(r,...)))
    if(length(row_res)==1)return(rep(row_res,mrow))
    if(length(row_res)==mrow)return(row_res)
    return(rep(0,length(r)))
  })
  dimnames(res)<-dimnames(m)
  res
}

"%-%"<-row_apply
"%|%"<-col_apply

################################################################################################################
################################################################################################################

# MOVING AVERAGE OF ONLY AVAILABLE DATA
ma_na <- function(r,w) {x = ms(scrub(r),w); y = ms(!is.na(r),w); ifelse(y, x/y, NA)}
moving_average_na <- function(n)function(x)ma_na(x,n)

# data is a daysXsecurities matrix of dayly returns.
# the %|% operator applies a function column-wise
# moving_average(window) returns a function that does it.
rolling_variance<-function(data,window){
  average_square <- (data*data)%|%moving_average_na(window)
  average_move <- data%|%moving_average_na(window)
  average_square - average_move*average_move
}

# Deals with 0s and NAs:
rolling_variance_nonzero<-function(data,window){
  data2 = (data*data)
  average_non_zero <- (data!=0)%|%moving_sum(window)
  average_square <- data2%|%moving_sum(window)
  average_move <- data%|%moving_sum(window)
  average_square = ifelse(average_non_zero, average_square/average_non_zero, 0)
  average_move = ifelse(average_non_zero, average_move/average_non_zero, 0)
  average_square - average_move*average_move
}

rolling_covariance<-function(security,hedge,window){
  comovement <- (security*hedge)%|%moving_average(window)
  product_of_means <- (security%|%moving_average(window))*(hedge%|%moving_average(window))
  comovement-product_of_means
}

rolling_correlation<-function(security1,security2,window){
  covariance<-rolling_covariance(security1,security2,window)
  variance1<-rolling_variance(security1,window)
  variance2<-rolling_variance(security2,window)
  ifelse(variance1>0&variance2>0,covariance/sqrt(variance1*variance2),0)
}

rolling_beta<-function(security,hedge,window){
  variance<-rolling_variance(hedge,window)
  covariance<-rolling_covariance(security,hedge,window)
  ifelse(variance>0,covariance/variance,0)
}

################################################################################################################
################################################################################################################
# DATASET CREATION RELATED FUNCTIONS
################################################################################################################
################################################################################################################

# Creates cross-sectional percentile based scores for a given company characteristic. 
get_cross_section_score <- function(company_feature_matrix, company_feature_matrix_used=NULL, zero_special = F, not_used = NULL){
  datacol = ncol(company_feature_matrix)
  data_used = company_feature_matrix
  data_used[data_used %in% not_used] <- NA
  if (!is.null(company_feature_matrix_used))
    data_used = cbind(company_feature_matrix,company_feature_matrix_used)
  tmp = t(apply(data_used,1,function(r){
    r_scored = r[1:datacol] 
    r_ecdf = r_scored
    if (!is.null(company_feature_matrix_used))
      r_ecdf = r[(datacol+1):length(r)]
    res = r_scored*NA
    
    if (!zero_special){
      r_scored = scrub(r_scored)
      r_ecdf = scrub(r_ecdf)
      if (sum(r_ecdf !=0)) {   # Note: we don't use both NAs and 0s
        score_fun = ecdf(r_ecdf[r_ecdf!=0])
        res = ifelse(r_scored!=0, score_fun(r_scored),NA)
      }
    } else {
      if (sum(!is.na(r_ecdf))) {   # Note: we don't use NAs only here
        score_fun = ecdf(r_ecdf[!is.na(r_ecdf)])
        res = ifelse(!is.na(r_scored), score_fun(r_scored),NA)
      }
    }
    res
  }))
  rownames(tmp) <- rownames(company_feature_matrix)
  tmp
}

################################################################################################################
################################################################################################################
# Simple analyses
################################################################################################################
################################################################################################################


# Returns the alpha, based on a running regression
# Riskfactors could be any (date x factor) vector or data frame and will be used in an lm:
#   lm("return ~ alpha + beta x factors")
# You can also specify a special "RF" factor in the df which will automatically go in the left-hand side:
#   lm("(return - RF) ~ factors)s
alpha_lm <- function(ri,Riskfactors,hedge_days, trade = 0) {
  #data check
  if (class(Riskfactors) != "data.frame") {
    dates <- names(Riskfactors)
    Riskfactors = data.frame(market=Riskfactors)
    rownames(Riskfactors) <- dates
  }
  
  #create the right formula
  RFfield    <- which(colnames(Riskfactors) == "RF")
  NotRFfield <- colnames(Riskfactors)[setdiff(1:ncol(Riskfactors),RFfield)]
  if(length(RFfield) == 1) {
    form <- as.formula(paste("(rj - RF) ~ ",paste(NotRFfield,collapse = "+")))
  } else {
    form <- as.formula(paste("rj ~ ",paste(NotRFfield,collapse = "+")))
  }
  #do a running coefficient estimation
  runcoeff <- function(rj) {
    data    = Riskfactors[rownames(Riskfactors) >= min(names(rj)) & rownames(Riskfactors) <= max(names(rj)),,drop=F]
    data$rj = rj
    model = RcppArmadillo::fastLm(form,data=data)
    return(summary(model)$coefficients[,1])
  }
  #names(ri) <- paste(stringr::str_sub(names(ri),start=1,end=7),"01",sep="-")
  coeff <- gtools::running(ri,fun=runcoeff,width=hedge_days,allow.fewer = T)
  coeffcorrection <- data.table::shift(t(coeff[NotRFfield,]),trade) * Riskfactors[,NotRFfield]
  if(dim(coeffcorrection)[1] !=1)
    coeffcorrection <- apply(coeffcorrection,1,sum)
  alpha <- (ri - coeffcorrection)
  if(length(RFfield) == 1)
    alpha <- alpha - Riskfactors[,RFfield]
  alpha <- alpha * (coeffcorrection!=0)*(ri!=0)
  return(alpha)
}

beta_expost <- function(ri,Riskfactors) {
  #data check
  if (class(Riskfactors) != "data.frame") {
    dates <- names(Riskfactors)
    Riskfactors = data.frame(market=Riskfactors)
    rownames(Riskfactors) <- dates
  }
  
  useonly = intersect(names(ri), rownames(Riskfactors))
  Riskfactors = Riskfactors[useonly,]
  ri = ri[useonly]
  
  #create the right formula
  RFfield    <- which(colnames(Riskfactors) == "RF")
  NotRFfield <- colnames(Riskfactors)[setdiff(1:ncol(Riskfactors),RFfield)]
  if(length(RFfield) == 1) {
    form <- as.formula(paste("(ri - RF) ~ ",paste(NotRFfield,collapse = "+")))
  } else {
    form <- as.formula(paste("ri ~ ",paste(NotRFfield,collapse = "+")))
  }
  
  data    = Riskfactors
  data$ri = ri
  model = fastLm(form,data=data)
  return(summary(model)$coefficients)
}

################################################################################################################
################################################################################################################
# Econometric methods
################################################################################################################
################################################################################################################


################################################################################################################
#Builds an IRATS table. Returns need to be monthly, 
# same for the risk factors. See description of method in the table captions of http://tevgeniou.github.io/BuybacksIssuers/
################################################################################################################
car_table <- function(returns,Event.Date,Risk_Factors_Monthly,min_window = -6, max_window = 48,formula_used="(ri - RF) ~ Delta + SMB + HML + RMW + CMA",value.weights = 1) {
  #data check
  if (class(Risk_Factors_Monthly) != "data.frame") {
    dates <- names(Risk_Factors_Monthly)
    Risk_Factors_Monthly = data.frame(market=Risk_Factors_Monthly)
    rownames(Risk_Factors_Monthly) <- dates
  }
  # Make all data have monthly dates on first of month:
  rownames(returns) <- paste(stringr::str_sub(rownames(Risk_Factors_Monthly), start=1,end=7),"01",sep="-")
  rownames(Risk_Factors_Monthly) <- paste(stringr::str_sub(rownames(Risk_Factors_Monthly), start=1,end=7),"01",sep="-")
  
  ###
  factors_used = setdiff(unlist(stringr::str_split(gsub("~", ",", gsub("\\-", ",", gsub("\\+", ",", gsub("\\)", "",gsub("\\(", "",formula_used))))), " , ")),"ri")
  min_window = min(1,min_window)
  max_window = max(-1,max_window)
  allmonths = min_window:max_window
  if (sum(!(factors_used %in% colnames(Risk_Factors_Monthly))))
    stop(paste("car_table misses the risk factors: ",factors_used[!(factors_used %in% colnames(Risk_Factors_Monthly))]))
  
  factors_used_noRF = setdiff(factors_used, "RF")
  if (ncol(returns) < length(factors_used_noRF) + 1){
    results = matrix(0,nrow=length(allmonths), ncol = 3)
    results = rbind(results, rep(ncol(returns)))
    betas = matrix(0,nrow=length(allmonths), ncol = length(factors_used_noRF))
    betasstderr = matrix(0,nrow=length(allmonths), ncol = length(factors_used_noRF)) 
    colnames(results) <- c("CAL","t-stat","p-value")
    rownames(results) <- c(ifelse(allmonths > 0, paste("+",allmonths,sep=""), allmonths),"Observations")
    colnames(results) <- c("CAR","t-stat","p-value")
    colnames(betas) <- factors_used_noRF
    colnames(betasstderr) <- factors_used_noRF
    all_results = list(results = results, betas = betas, betasstderr = betasstderr)
    return(all_results) 
  }
  
  #Step 1: Build an event matrix, where all the events are aligned by event month as opposed to calendar month
  Event.Date = paste(stringr::str_sub(Event.Date,start=1,end=7), "01", sep="-") # just in case
  Row.Date <- as.Date(rownames(returns))
  Row.Date_number = as.numeric(Row.Date)
  Event.Date_number = as.numeric(as.Date(Event.Date))
  firstday = as.numeric(as.Date(head(rownames(returns),1)))
  lastday = as.numeric(as.Date(tail(rownames(returns),1)))
  
  firstHit = match(Event.Date_number,Row.Date_number)
  form  = as.formula(formula_used)
  Risk_Factors_Monthly = Risk_Factors_Monthly[,factors_used]
  if (!("RF" %in% factors_used)){
    Risk_Factors_Monthly = cbind(Risk_Factors_Monthly,matrix(0,nrow=nrow(Risk_Factors_Monthly)))
    colnames(Risk_Factors_Monthly)[ncol(Risk_Factors_Monthly)] <- "RF"
  }
  
  #Build event_month x company x factor matrix
  EVENT_ALIGNED <- array(0, c(length(allmonths), ncol(returns), ncol(Risk_Factors_Monthly)+1)) 
  starting      = pmax(1,pmin(nrow(returns),firstHit + min_window))
  ending        = pmax(1,pmin(nrow(returns),firstHit + max_window))
  Risk_Factors_Matrix <- as.matrix(Risk_Factors_Monthly)
  # THIS IS THE SLOW PART
  for(ev in 1:length(Event.Date)) {
    EVENT_ALIGNED[(1 + (starting[ev] - (firstHit[ev] + min_window))):(length(allmonths) - (firstHit[ev] + max_window-ending[ev])),ev,] <- cbind(Risk_Factors_Matrix[starting[ev]:ending[ev],], returns[starting[ev]:ending[ev],ev])
  }
  dimnames(EVENT_ALIGNED) <- list( ifelse(allmonths > 0, paste("+",allmonths,sep=""), allmonths),
                                   c(paste(colnames(returns),Event.Date_number)),
                                   c(colnames(Risk_Factors_Monthly),"ri"))
  
  #Step2: now for each month, calculate the CAR. need to include month 0 and set that one to 0  
  alphas <- rep(0,length(allmonths))
  betas <- array(0,c(length(allmonths),ncol(Risk_Factors_Monthly)-1))
  betasstderr <- array(0,c(length(allmonths),ncol(Risk_Factors_Monthly)-1))
  stderr <- rep(0,length(allmonths))
  dfs <- as.integer(rep(0,length(allmonths)))
  event_alphas = matrix(0,nrow = length(Event.Date_number), ncol = length(allmonths))
  
  match_ini =  match(Event.Date_number, Row.Date_number)
  for (i in 1:length(allmonths)) {
    #cat(i,"")
    if (allmonths[i] !=0) { #we do not consider the month of the event
      hitnow = match_ini + allmonths[i]
      ret <- EVENT_ALIGNED[i,,]
      non_zeros = which(ret[,"ri"]!=0 & (hitnow <= length(Row.Date_number)) & (hitnow >= 1))
      ret <- ret[non_zeros,]  # WE NEED THIS HERE!!!!!
      
      alphas[i] = 0
      betas[i,] <- 0
      betasstderr[i,] <- 0  
      stderr[i] = 0
      dfs[i] = 0
      event_alphas[non_zeros,i] <- 0
      
      if (!is.null(dim(ret))){ # need more than just one row
        if (nrow(ret) > ncol(ret)){
          model = fastLm(form,data=data.frame(ret,row.names = NULL))
          model.summary = summary(model)
          
          alphas[i] = model.summary$coefficients[1,"Estimate"] 
          stderr[i] = model.summary$coefficients[1, "StdErr"]
          
          the_betas = model.summary$coefficients[2:nrow(model.summary$coefficients), "Estimate"]
          betas[i,] <- the_betas
          betasstderr[i,] <- model.summary$coefficients[2:nrow(model.summary$coefficients), "StdErr"]    
          dfs[i] = df.residual(model)
          event_alphas[non_zeros,i] <- ret[,"ri"] - ret[,"RF"] - ret[,names(the_betas)]%*%matrix(the_betas,ncol=1)
        } 
      }
    }
  }
  event_alphas_aggregate = 0*event_alphas
  #summary CAR output: aggregate results for all windows
  results <- array(0,c(length(allmonths)+1,3))
  if (0 %in% allmonths){
    for (i in 1:(which(allmonths==0)-1)){
      thealpha = sum(alphas[(which(allmonths==0)-1):i]) # this is i:1 since we want to end up with a 0 at event month - 1 (and at event month)
      thestderr = sqrt(sum(stderr[(which(allmonths==0)-1):i]*stderr[(which(allmonths==0)-1):i])) # this is 1:i since we want to add them all from the oldest month
      tstat = ifelse(thestderr, thealpha / thestderr, 0) 
      results[i,] <- c( 
        thealpha,  
        tstat,
        2 * pt(abs(tstat), df = dfs[i], lower.tail = FALSE)    #pvalue
      )
    }
    for (i in (which(allmonths==0)+1):length(allmonths)){
      thealpha = sum(alphas[(which(allmonths==0)+1):i])
      thestderr = sqrt(sum(stderr[(which(allmonths==0)+1):i]*stderr[(which(allmonths==0)+1):i]))
      tstat = ifelse(thestderr, thealpha / thestderr, 0) 
      results[i,] <- c( 
        thealpha,  
        tstat,
        2 * pt(abs(tstat), df = dfs[i], lower.tail = FALSE)    #pvalue
      )
    }
  } else {
    for (i in 1:length(allmonths)){
      thealpha = sum(alphas[1:i])
      thestderr = sqrt(sum(stderr[1:i]*stderr[1:i]))
      tstat = ifelse(thestderr, thealpha / thestderr, 0) 
      results[i,] <- c( 
        thealpha,  
        tstat,
        2 * pt(abs(tstat), df = dfs[i], lower.tail = FALSE)    #pvalue
      )
    }
  }
  results[,1] = 100*results[,1]
  rownames(results) <- c(ifelse(allmonths > 0, paste("+",allmonths,sep=""), allmonths),"Observations")
  colnames(results) <- c("CAR","t-stat","p-value")
  results[nrow(results),] <- rep(length(Event.Date_number),ncol(results)) 
  colnames(betas) <- factors_used_noRF
  colnames(betasstderr) <- factors_used_noRF
  all_results = list(results = results, betas = betas, betasstderr = betasstderr,event_alphas = event_alphas,dfs = dfs)
  return(all_results)
}


################################################################################################################
# Builds a Calendar Time table. Returns need to be monthly, 
# same for the risk factors. See description of method in the table captions of http://tevgeniou.github.io/BuybacksIssuers/
################################################################################################################
calendar_table <- function(returns,Event.Date, Risk_Factors_Monthly,min_window = -6, max_window = 48,formula_used="(ri - RF) ~ Delta + SMB + HML + RMW + CMA",value.weights = 1) {
  #data check
  if (class(Risk_Factors_Monthly) != "data.frame") {
    dates <- names(Risk_Factors_Monthly)
    Risk_Factors_Monthly = data.frame(market=Risk_Factors_Monthly)
    rownames(Risk_Factors_Monthly) <- dates
  }
  # Make all data have monthly dates on first of month:
  rownames(returns) <- paste(stringr::str_sub(rownames(Risk_Factors_Monthly), start=1,end=7),"01",sep="-")
  rownames(Risk_Factors_Monthly) <- paste(stringr::str_sub(rownames(Risk_Factors_Monthly), start=1,end=7),"01",sep="-")
  
  ### 
  factors_used = setdiff(unlist(stringr::str_split(gsub("~", ",", gsub("\\-", ",", gsub("\\+", ",", gsub("\\)", "",gsub("\\(", "",formula_used))))), " , ")),"ri")
  factors_used_noRF = setdiff(factors_used, "RF")
  min_window = min(1,min_window)
  max_window = max(-1,max_window)
  allmonths = min_window:max_window
  if (sum(!(factors_used %in% colnames(Risk_Factors_Monthly))))
    stop(paste("calendar_table misses the risk factors: ",factors_used[!(factors_used %in% colnames(Risk_Factors_Monthly))]))
  
  if (ncol(returns) < length(factors_used_noRF) + 1){
    results = matrix(0,nrow=length(allmonths), ncol = 3)
    results = rbind(results, rep(ncol(returns),3))
    betas = matrix(0,nrow=length(allmonths), ncol = length(factors_used_noRF))
    betasstderr = matrix(0,nrow=length(allmonths), ncol = length(factors_used_noRF)) 
    dfs <- as.integer(rep(0,length(allmonths)))
    colnames(results) <- c("CAL","t-stat","p-value")
    rownames(results) <- c(ifelse(allmonths > 0, paste("+",allmonths,sep=""), allmonths),"Observations")
    colnames(results) <- c("CAR","t-stat","p-value")
    colnames(betas) <- factors_used_noRF
    colnames(betasstderr) <- factors_used_noRF
    all_results = list(results = results, betas = betas, betasstderr = betasstderr)
    return(all_results) 
  }
  
  Row.Date <- as.Date(rownames(returns))
  form  = as.formula(formula_used)
  Event.Date = paste(stringr::str_sub(Event.Date,start=1,end=7), "01", sep="-") # just in case
  Risk_Factors_Monthly = Risk_Factors_Monthly[,factors_used]
  if (!("RF" %in% factors_used)){
    Risk_Factors_Monthly = cbind(Risk_Factors_Monthly,matrix(0,nrow=nrow(Risk_Factors_Monthly)))
    colnames(Risk_Factors_Monthly)[ncol(Risk_Factors_Monthly)] <- "RF"
  }
  
  alphas <- rep(0,length(allmonths))
  betas <- array(0,c(length(allmonths),ncol(Risk_Factors_Monthly)-1))
  betasstderr <- array(0,c(length(allmonths),ncol(Risk_Factors_Monthly)-1))
  stderr <- rep(0,length(allmonths))
  dfs <- as.integer(rep(0,length(allmonths)))
  
  Event.Date_number = as.numeric(as.Date(Event.Date))
  Row.Date_number = as.numeric(Row.Date)
  #Now create aggregate returns based on the window and calculate the model
  results <- array(0,c(length(allmonths)+1,3))
  max_index = length(Row.Date_number)
  start_match = match(Event.Date_number,Row.Date_number)
  
  for (i in 1:length(allmonths)) {
    w = allmonths[i]
    hitnow = Row.Date_number[pmax(1,pmin(start_match+w,max_index))]
    ret <- returns
    if (w > 0) ret <- calendar_table_helper1(ret, Row.Date_number, Event.Date_number, hitnow)
    #for(j in 1:length(Event.Date)) 
    #   ret[ Row.Date_number <= Event.Date_number[j] | Row.Date_number > hitnow[j],j ] <- 0
    if (w < 0) ret <- calendar_table_helper2(ret, Row.Date_number, Event.Date_number, hitnow)
    # for(j in 1:length(Event.Date)) 
    #   ret[ Row.Date_number >= Event.Date_number[j] | Row.Date_number < hitnow[j],j ] <- 0
    
    ri <- row_weights(ret, value.weights)
    
    if (sum(ri!=0) > 10) { ### For special cases...
      data  = Risk_Factors_Monthly
      
      #ri <- ri[ head(which(ri !=0),1) : tail(which(ri!=0),1)]
      #data  = data[rownames(data) >= min(names(ri)) & rownames(data) <= max(names(ri)),]
      non_zero_months = which(ri!=0)
      ri <- ri[non_zero_months]
      data = data[non_zero_months,]
      
      data$ri = ri
      form  = as.formula(form)
      model = fastLm(form,data=data)
      
      results[i,] <- c( 
        summary(model)$coefficients[1,"Estimate"]*100,
        summary(model)$coefficients[1,"t.value"],
        summary(model)$coefficients[1,"p.value"]
      )
      betas[i,] <- summary(model)$coefficients[2:nrow(summary(model)$coefficients), "Estimate"]
      betasstderr[i,] <- summary(model)$coefficients[2:nrow(summary(model)$coefficients), "StdErr"]  
      dfs[i] = df.residual(model)
      
    }
  }
  
  rownames(results) <- c(ifelse(allmonths > 0, paste("+",allmonths,sep=""), allmonths),"Observations")
  colnames(results) <- c("CAL","t-stat","p-value")
  results[nrow(results),] <- rep(length(Event.Date),ncol(results)) 
  colnames(betas) <- factors_used_noRF
  colnames(betasstderr) <- factors_used_noRF
  all_results = list(results = results, betas = betas, betasstderr = betasstderr,dfs=dfs)
  return(all_results)
}

####################################################################################
#Builds a stock specific regression like Brennan, Chordia and Subrahmanyam (1998)
####################################################################################
# Split in two parts as the first one is more generic 

# PART I OF BSC1998_event_study: This is a general function that gets the betas and actual returns for a series of months for the events
event_study_factor_coeffs <- function(returns,Event.Date, Risk_Factors_Monthly,formula_used="(ri - RF) ~ Delta + SMB + HML + RMW + CMA", rolling_window=60, timeperiods_requested = 1:48, min_data = 10){
  # assumes returns has one column per event, so number of columns equal to Event.Date. Rows are months
  # Keep track also of the risk and stock returns the next month - where we will do the predictions
  factors_used = setdiff(unlist(stringr::str_split(gsub("~", ",", gsub("\\-", ",", gsub("\\+", ",", gsub("\\)", "",gsub("\\(", "",formula_used))))), " , ")),"ri")
  if (sum(!(factors_used %in% colnames(Risk_Factors_Monthly))))
    stop(paste("BSC1998_event_study misses the risk factors: ",factors_used[!(factors_used %in% colnames(Risk_Factors_Monthly))]))
  
  factors_used_noRF = setdiff(factors_used, "RF")
  number_of_factors = length(factors_used_noRF)
  form  = as.formula(formula_used)
  Risk_Factors_Monthly = Risk_Factors_Monthly[,factors_used]
  if (!("RF" %in% factors_used)){
    Risk_Factors_Monthly = cbind(Risk_Factors_Monthly,matrix(0,nrow=nrow(Risk_Factors_Monthly)))
    colnames(Risk_Factors_Monthly)[ncol(Risk_Factors_Monthly)] <- "RF"
  }
  
  
  #Step 1: Build an event matrix, where all the events are aligned by event month as opposed to calendar month
  returns_month = stringr::str_sub(rownames(returns), start=1,end=7)
  Event.Date = stringr::str_sub(Event.Date,start=1,end=7)
  firstHit = match(Event.Date,returns_month)
  if (sum(is.na(firstHit)) !=0)
    print("\nSOME EVENTS SEEM TO BE OUTISDE THE RANGE OF DATES OF THE RISK FACTORS...\n")
  res_init = structure(rep(NA,number_of_factors + (number_of_factors+1) + 1), .Names = c(factors_used_noRF, paste(factors_used,"ret", sep="_"), "actual_ret"))
  max_time = length(returns_month)
  
  ## Step 1: Estimate Factor Loadings for the requested months (month 0 is the event month)
  lapply(1:length(Event.Date), function(i){
    if (i%%50 == 1) cat(i,",")
    stock_returns = structure(returns[,i], .Names = returns_month)
    event_row = firstHit[i]
    stock_factors = t(Reduce(cbind,lapply(timeperiods_requested, function(themonth){ 
      #cat(themonth,",")
      res = res_init
      period_used = min(max_time, max(1,event_row+themonth - rolling_window)):min(max_time, max(1,event_row+themonth -1)) # We use -1 but can also use -2 to use till two months before
      period_to_predict = min(max_time, max(1,event_row+themonth))
      
      if (length(period_used) > min_data & tail(period_used,1) < period_to_predict){ # in case we are at the last month available for this stocks
        ret = cbind(Risk_Factors_Monthly,stock_returns)[period_used,] # Ignore NAs and 0s
        colnames(ret)[ncol(ret)]<- "ri"
        if (sum(!is.na(ret[,"ri"])) >= min_data){
          ret <- ret[!is.na(ret[,"ri"]),] 
          model = fastLm(form,data=data.frame(ret,row.names = NULL))
          thebetas = summary(model)$coefficients[2:nrow(summary(model)$coefficients), "Estimate"]
          res = c(thebetas,as.numeric(Risk_Factors_Monthly[period_to_predict,]), stock_returns[period_to_predict])
          names(res) <- c(factors_used_noRF, paste(factors_used,"ret", sep="_"), "actual_ret")
        }
      }
      res
    })))
    rownames(stock_factors) <- timeperiods_requested
    stock_factors
  })
}

# PART II OF BSC1998_event_study
BSC1998_event_study_coeffs <- function(Estimated_returns,company_features,timeperiods_requested = 1:48,square_features=NULL,nomissing_allowed,  
                                       instrumental_var_endogenous = NULL, instrumental_var_IVs = NULL, company_features_instrumental = NULL, 
                                       remove_vars_report = function(varname) !stringr::str_detect(varname,"dummies")){
  # assumes returns has one column per event, so number of columns equal to Event.Date. Rows are months
  # Assumes factor_loadings comes from event_study_factor_coeffs using the exact same inputs
  ## Step 1: Estimate Factor Loadings for the requested months (so month 0 is the event month)
  # Assumes these are inputs: It is done using the function event_study_factor_coeffs above
  ## Step 2: Calculate Monthly Estimated Risk-adjusted Return (Given as input)
  ## Step 3: Run Cross-Section Regression in Each Post-Event Month, from 1-48 months
  rownames(company_features) <- rownames(Estimated_returns)
  colnames(company_features) <- sapply(colnames(company_features), function(i){
    while(stringr::str_detect(i, " ")) i = stringr::str_replace(i, " ","_")
    while(stringr::str_detect(i, "\\(")) i = stringr::str_replace(i, "\\(","_")
    while(stringr::str_detect(i, "\\)")) i = stringr::str_replace(i, "\\)","_")
    while(stringr::str_detect(i, "\\+")) i = stringr::str_replace(i, "\\+","_")
    while(stringr::str_detect(i, "-")) i = stringr::str_replace(i, "-","_")
    while(stringr::str_detect(i, "/")) i = stringr::str_replace(i, "/","_")
    while(stringr::str_detect(i, "\\*")) i = stringr::str_replace(i, "\\*","_")
    i}) 
  
  
  if (!is.null(nomissing_allowed))
    nomissing_allowed = intersect(nomissing_allowed, colnames(company_features))
  if (!is.null(square_features))
    square_features = intersect(square_features, colnames(company_features))
  
  inst_results = NULL
  
  # Replace actual with instrumental var (for 1 instrumental var)
  if (!is.null(instrumental_var_endogenous)){
    rownames(company_features_instrumental) <- rownames(Estimated_returns)
    colnames(company_features_instrumental) <- sapply(colnames(company_features_instrumental), function(i){
      while(stringr::str_detect(i, " ")) i = stringr::str_replace(i, " ","_")
      while(stringr::str_detect(i, "\\(")) i = stringr::str_replace(i, "\\(","_")
      while(stringr::str_detect(i, "\\)")) i = stringr::str_replace(i, "\\)","_")
      while(stringr::str_detect(i, "\\+")) i = stringr::str_replace(i, "\\+","_")
      while(stringr::str_detect(i, "-")) i = stringr::str_replace(i, "-","_")
      while(stringr::str_detect(i, "/")) i = stringr::str_replace(i, "/","_")
      while(stringr::str_detect(i, "\\*")) i = stringr::str_replace(i, "\\*","_")
      i}) 
    instrumental_var_IVs <- sapply(instrumental_var_IVs,function(i){
      while(stringr::str_detect(i, " ")) i = stringr::str_replace(i, " ","_")
      while(stringr::str_detect(i, "\\(")) i = stringr::str_replace(i, "\\(","_")
      while(stringr::str_detect(i, "\\)")) i = stringr::str_replace(i, "\\)","_")
      while(stringr::str_detect(i, "\\+")) i = stringr::str_replace(i, "\\+","_")
      while(stringr::str_detect(i, "-")) i = stringr::str_replace(i, "-","_")
      while(stringr::str_detect(i, "/")) i = stringr::str_replace(i, "/","_")
      while(stringr::str_detect(i, "\\*")) i = stringr::str_replace(i, "\\*","_")
      i})
    
    
    cross_setional_form = paste(instrumental_var_endogenous, paste(instrumental_var_IVs, collapse=" + "), sep=" ~ ")
    useonly_instr = which(apply(company_features_instrumental,1,function(r) sum(is.na(r))==0))
    #useonly_instr = which(apply(company_features_instrumental[,c(instrumental_var_endogenous,instrumental_var_IVs)],1,function(r) sum(is.na(r))==0))
    company_features_instrumental <- company_features_instrumental[useonly_instr,]
    Estimated_returns = Estimated_returns[useonly_instr,]
    company_features = company_features[useonly_instr,]
    simple.ed.1s <- lm(cross_setional_form, data=company_features_instrumental)
    # replace actual with predicted variable
    tmp = predict(simple.ed.1s)
    
    # keep instrument strength ets
    cross_setional_form <- stringr::str_replace(cross_setional_form," \\+ instrumental_var", "")
    instr.strength.regression <- lm(cross_setional_form, data=company_features_instrumental)
    inst_results =  list(
      inst_strength = c(waldtest(simple.ed.1s, instr.strength.regression)$F[2],
                        waldtest(simple.ed.1s, instr.strength.regression, vcov = vcovHC(simple.ed.1s, type="HC0"))$F[2],
                        cor(tmp,company_features[,instrumental_var_endogenous])),
      inst_model= simple.ed.1s
    )
    
    # replace the actual with the predicted main variable
    company_features[,instrumental_var_endogenous] <- tmp
    
  }
  res = Reduce(rbind,lapply(timeperiods_requested, function(tmonth){
    alpha_t = Estimated_returns[,tmonth]
    company_features_withret = cbind(alpha_t,company_features)
    colnames(company_features_withret)[1] <- "alphaT"
    nomissing_allowed = union(nomissing_allowed,"alphaT")
    if (!is.null(nomissing_allowed))
      company_features_withret <- company_features_withret[!is.na(apply(company_features_withret[,nomissing_allowed, drop=F], 1, sum)), ]
    for (square_feature_id in square_features){
      tmp = company_features_withret[,square_feature_id]
      tmp = cbind(tmp - mean(tmp), (tmp - mean(tmp))*(tmp - mean(tmp)))
      colnames(tmp)<- paste(square_feature_id, c("Demean","DemeanSquare"), sep="")
      company_features_withret = cbind(company_features_withret[,setdiff(colnames(company_features_withret),square_feature_id ), drop=F],tmp)
    }
    company_features_withret <- company_features_withret[apply(company_features_withret,1,function(r) sum(is.na(r))==0),]
    #cat(nrow(company_features_withret), " ")
    cross_setional_form = paste("alphaT", paste(setdiff(colnames(company_features_withret), "alphaT"), collapse=" + "), sep=" ~ ")
    model = fastLm(as.formula(cross_setional_form),data=data.frame(company_features_withret,row.names = NULL))
    tmp = summary(model)$coefficients[,1]
    if (!is.null(remove_vars_report))
      tmp <- tmp[remove_vars_report(names(tmp))]
    
    if (0){ # Another way to do IVR
      instrumental_var = instrumental_var_ini[rownames(company_features_withret)]
      company_features_withret$instrumental_var = instrumental_var
      cross_setional_form = paste(cross_setional_form, paste(" . - ",instrumental_var_endogenous, " + ", "instrumental_var", collapse=""), sep=" | ")
      model <- ivreg(cross_setional_form,data = data.frame(company_features_withret,row.names = NULL))
      tmp = summary(model)$coefficients[,1]
      if (!is.null(remove_vars_report))
        tmp <- tmp[remove_vars_report(names(tmp))]
    }
    
    tmp
  }))
  
  colnames(res)[1] <- "Intercept"
  rownames(res) <- timeperiods_requested
  if (is.null(instrumental_var_endogenous))
    finalres = res
  else
    finalres= list(res = res, inst_results = inst_results)
  finalres
}


## Step 4: Aggregate  C_mt_coefficients over 48 Post-Event Months: Time-Series Average of C_mt_coefficients (This can be done outside, for whatever months one needs)
BSC1998_coeffs_tmp_aggregator <- function(BSC1998_coefficients,the_months_needed = c("12", "24", "36","48")){
  BSC1998_coeffs = t(Reduce(cbind,lapply(the_months_needed, function(i){
    apply(BSC1998_coefficients,2,function(r){
      useonly = which(rownames(BSC1998_coefficients)=="1"):which(rownames(BSC1998_coefficients)==i)
      mean(r[useonly])
    }) 
  })))
  rownames(BSC1998_coeffs) <- paste("month", the_months_needed)
  
  BSC1998_tstats = t(Reduce(cbind,lapply(the_months_needed, function(i){
    apply(BSC1998_coefficients,2,function(r){
      useonly = which(rownames(BSC1998_coefficients)=="1"):which(rownames(BSC1998_coefficients)==i)
      t.test(r[useonly])$statistic
    }) 
  })))
  rownames(BSC1998_tstats) <- paste("tstat: month", the_months_needed)
  
  BSC1998_pvalue = t(Reduce(cbind,lapply(the_months_needed, function(i){
    apply(BSC1998_coefficients,2,function(r){
      useonly = which(rownames(BSC1998_coefficients)=="1"):which(rownames(BSC1998_coefficients)==i)
      t.test(r[useonly])$p.value
    }) 
  }))) 
  rownames(BSC1998_pvalue) <- paste("pvalue: month", the_months_needed)
  
  BSC1998<- Reduce(rbind,lapply(1:nrow(BSC1998_coeffs), function(i) {res = rbind(1*BSC1998_coeffs[i,],BSC1998_tstats[i,],BSC1998_pvalue[i,]); rownames(res)<-c(rownames(BSC1998_coeffs)[i], rownames(BSC1998_tstats)[i],rownames(BSC1998_pvalue)[i]); res}))
  t(BSC1998)
}

################################################################################################################
#####################################################################################
### ADDITIONAL KEY CODE FROM BUYBACKS/ISSUERS PROJECT
#####################################################################################
################################################################################################################

#Normal performance Function
Performance <- function(date_to_start, date_to_end, Dates, returns_used, event){
  Start_Date <- Dates[which(rownames(Dates) == date_to_start),]
  End_Date <- Dates[which(rownames(Dates) == date_to_end),]
  
  if (event)
    res = Reduce(cbind,lapply(1:length(Start_Date), function(i) 252*non_zero_mean(returns_used[rownames(returns_used) >= Start_Date[i] & rownames(returns_used) <= End_Date[i],i])))
  if (!event)
    res = Reduce(cbind,lapply(1:length(End_Date), function(i) 252*non_zero_mean(returns_used[(rownames(returns_used) > End_Date[i] | rownames(returns_used) < Start_Date[i]),i])))
  res
}

#PNL Matrix function (for any subset of companies to use)
PNL_matrix_BB <- function(date_to_start, date_to_end, company_subset, Dates, returns_used, event=1){
  returns_used <- returns_used[,company_subset, drop=F]
  Start_Date <- Dates[which(rownames(Dates) == date_to_start),company_subset]
  End_Date <- Dates[which(rownames(Dates) == date_to_end),company_subset]
  
  dates_used = rownames(returns_used)
  if (event){
    dates_matrix= mapply(function(s,e){
      dates_used >= s & dates_used <= e
    },Start_Date, End_Date)
    res = returns_used*dates_matrix  
  }
  if (!event){
    dates_matrix = Reduce(cbind,lapply(1:length(Start_Date), function(i) (dates_used > End_Date[i]))) 
    res = returns_used*dates_matrix  
  }
  res
}


Betas <- function(x,y,Dates, EVENT_RETURNS,Riskfactors){
  if (class(Riskfactors) != "matrix")
    Riskfactors = matrix(Riskfactors,ncol=1)
  Start_Date <- Dates[which(rownames(Dates) == x),]
  End_Date <- Dates[which(rownames(Dates) == y),]
  non_zeros <- which(apply(Riskfactors!=0,1,sum)!=0) 
  Riskfactors <- Riskfactors[non_zeros,,drop=F]
  tmp_matrix <- EVENT_RETURNS[non_zeros,]
  res = Reduce(cbind,lapply(1:length(Start_Date), function(i){
    thebetas = rep(0,ncol(Riskfactors))
    returns_tmp = tmp_matrix[rownames(tmp_matrix) >= Start_Date[i] & rownames(tmp_matrix) <= End_Date[i],i]
    RiskF_tmp = Riskfactors[which(rownames(tmp_matrix) >= Start_Date[i] & rownames(tmp_matrix) <= End_Date[i]),,drop=F]
    individualVariance = apply(RiskF_tmp,2,sd)
    useonly_nonzero_factors = which(individualVariance !=0 )
    if (length(useonly_nonzero_factors) > 0){
      RiskF_tmp_nonzero = RiskF_tmp[,useonly_nonzero_factors,drop=F]
      # REPLACE THESE THREE LINES WITH R'S OLS... to be sure/robust...
      thecovariance = cov(returns_tmp, RiskF_tmp_nonzero)
      thevariance = var(RiskF_tmp_nonzero)
      thebetas[useonly_nonzero_factors] <- solve(thevariance + 0*diag(ncol(thevariance)))%*%matrix(thecovariance,ncol=1) # maybe add a small diagonal for stability?
    }
    thebetas
  }))
  rownames(res)<- colnames(Riskfactors)
  res
}

create_dates_month <- function(Event_Date, allmonths) {
  last_available = tail(allmonths,1)
  last_available = paste(format(DescTools::AddMonths(as.Date(last_available),1),"%Y-%m"), "01",sep="-") # Just make it a date in the future
  allmonths = sort(allmonths) # just in case
  allmonths = stringr::str_sub(allmonths, start=1,end=7)
  tmp = match(stringr::str_sub(Event_Date, start = 1, end = 7), stringr::str_sub(allmonths, start=1,end=7))
  
  Trading.Day = ifelse(!is.na(tmp), paste(allmonths[pmin(tmp+1, length(allmonths))], "01",sep="-"), last_available)
  One.Month.After = ifelse(!is.na(tmp), paste(allmonths[pmin(tmp+1+1, length(allmonths))], "01",sep="-"), last_available)
  Three.Month.After = ifelse(!is.na(tmp), paste(allmonths[pmin(tmp+1+3, length(allmonths))], "01",sep="-"), last_available)
  Six.Month.After = ifelse(!is.na(tmp), paste(allmonths[pmin(tmp+1+6, length(allmonths))], "01",sep="-"), last_available)
  One.Year.After = ifelse(!is.na(tmp), paste(allmonths[pmin(tmp+1+12, length(allmonths))], "01",sep="-"), last_available)
  Two.Years.After = ifelse(!is.na(tmp), paste(allmonths[pmin(tmp+1+24, length(allmonths))], "01",sep="-"), last_available)
  Three.Years.After = ifelse(!is.na(tmp), paste(allmonths[pmin(tmp+1+36, length(allmonths))], "01",sep="-"), last_available)
  Four.Years.After = ifelse(!is.na(tmp), paste(allmonths[pmin(tmp+1+48, length(allmonths))], "01",sep="-"), last_available)
  Dates <- data.frame(Trading.Day,One.Month.After,Three.Month.After,Six.Month.After,One.Year.After,Two.Years.After,Three.Years.After,Four.Years.After)
  colnames(Dates) <- c("Trading.Day","One.Month.After","Three.Month.After","Six.Month.After","One.Year.After","Two.Years.After","Three.Years.After","Four.Years.After")
  Dates <- t(Dates)
  return(Dates)
}

is.crisis <- function(thedate, crisis_years, slack_pre=0, slack_post = 0)
  sum(sapply(1:length(crisis_years), function(i) thedate >= DescTools::AddMonths(crisis_years[[i]][1], - slack_pre*12) & thedate <= DescTools::AddMonths(crisis_years[[i]][2], slack_post*12)))!=0

plot_crisis_dates <- function(all_ret_values, monthly = 1){
  if (class(all_ret_values) == "matrix" | class(all_ret_values) == "data.frame"){
    thenames = rownames(all_ret_values) 
  } else {
    thenames = names(all_ret_values)
  }  
  abline(h=0)
  for (i in 1:length(BEAR_YEARS)){
    if (!monthly){
      abline(v=which.min(abs(as.Date(thenames) - BEAR_YEARS[[i]][1])),lwd=3, col= "black", lty="solid")
      abline(v=which.min(abs(as.Date(thenames) - BEAR_YEARS[[i]][2])),lwd=3, col= "red", lty="solid")    
    } else {
      abline(v=which.min(abs(as.numeric(format(as.Date(thenames), "%Y%m")) - as.numeric(format(as.Date(BEAR_YEARS[[i]][1]), "%Y%m")))),lwd=3, col= "black", lty="solid")
      abline(v=which.min(abs(as.numeric(format(as.Date(thenames), "%Y%m")) - as.numeric(format(as.Date(BEAR_YEARS[[i]][2]), "%Y%m")))),lwd=3, col= "red", lty="solid")
    }
  }
}

################################################################################################################
################################################################################################################
# DATASET CREATION RELATED FUNCTIONS
################################################################################################################
################################################################################################################


# A helper that creates yearly matrices for each firm characteristic,
# aligned with the monthly data. Note the fiscal year use
# (datadate is the end of the fiscal year)
create_yearly_data <- function(value_used, template_matrix,all_compustat_data){
  tmp_data = as.data.frame(dcast(all_compustat_data, datadate ~ LPERMNO,
                                 fun.aggregate = function(r) ifelse(length(unique(r)) > 1, NA, unique(r)),
                                 value.var=value_used)) 
  # Convert tmp_data to matrix with dates as rownames
  tmp = as.character(tmp_data$datadate)
  tmp_data$datadate <- NULL
  tmp_data <- as.matrix(tmp_data)
  rownames(tmp_data) <- tmp
  # Align with monthly data now
  rownames(tmp_data) <- rownames(
    template_matrix)[match(
      str_sub(rownames(tmp_data),start=1,end=7),
      str_sub(rownames(template_matrix),start=1,end=7))]
  tmp_data = tmp_data[,intersect(colnames(tmp_data),
                                 colnames(template_matrix))]
  res = NA*template_matrix
  res[rownames(tmp_data),colnames(tmp_data)] <- tmp_data  
  # Fill in the gaps
  # we need to start using the data from the "next month",
  # after the fiscal year end
  res = apply(res,2,function(r) {
    x = fill_NA_previous(r);
    if(is.na(tail(x,1)) & sum(!is.na(x))) {
      x[pmin(length(x),tail(which(!is.na(x)),1):
               (tail(which(!is.na(x)),1)+11))] <- x[tail(which(!is.na(x)),1)]
    } 
    c(NA,head(x,-1))
  }) 
  rownames(res) <- rownames(template_matrix)
  res
}

# Creates cross-sectional percentile based scores for a given company characteristic. 
get_cross_section_score <- function(company_feature_matrix, company_feature_matrix_used=NULL, zero_special = F, not_used = NULL){
  datacol = ncol(company_feature_matrix)
  data_used = company_feature_matrix
  data_used[data_used %in% not_used] <- NA
  if (!is.null(company_feature_matrix_used))
    data_used = cbind(company_feature_matrix,company_feature_matrix_used)
  tmp = t(apply(data_used,1,function(r){
    r_scored = r[1:datacol] 
    r_ecdf = r_scored
    if (!is.null(company_feature_matrix_used))
      r_ecdf = r[(datacol+1):length(r)]
    res = r_scored*NA
    
    if (!zero_special){
      r_scored = scrub(r_scored)
      r_ecdf = scrub(r_ecdf)
      if (sum(r_ecdf !=0)) {   # Note: we don't use both NAs and 0s
        score_fun = ecdf(r_ecdf[r_ecdf!=0])
        res = ifelse(r_scored!=0, score_fun(r_scored),NA)
      }
    } else {
      if (sum(!is.na(r_ecdf))) {   # Note: we don't use NAs only here
        score_fun = ecdf(r_ecdf[!is.na(r_ecdf)])
        res = ifelse(!is.na(r_scored), score_fun(r_scored),NA)
      }
    }
    res
  }))
  rownames(tmp) <- rownames(company_feature_matrix)
  tmp
}

# Creates cross-sectional percentile based scores for a given company characteristic PER INDUSTRY
# ASSUMES company_feature_matrix, company_feature_matrix_used, industry_matrix have the same number of colunns!! (for now - needs fixing/clean up)
# All inputs must be numeric
get_cross_section_score_industry <- function(company_feature_matrix, company_feature_matrix_used=NULL,industry_matrix, zero_special = F, not_used = NULL){
  
  # zero_special is FALSE if we also score the 0s. When zero_special is TRUE then we don't score the 0s. 
  company_feature_matrix[company_feature_matrix %in% not_used] <- NA
  if (is.null(company_feature_matrix_used))
    company_feature_matrix_used = company_feature_matrix
  
  resall = company_feature_matrix[1,]*NA
  names(resall) <- 1:length(resall)
  
  tmp = Reduce(rbind,lapply(1:nrow(company_feature_matrix), function(iter){
    r_scored = company_feature_matrix[iter,] 
    r_ecdf = company_feature_matrix_used[iter,]
    r_industry = industry_matrix[iter,] 
    # !!! ************************ Assumes same length!!! ************************
    names(r_scored) <- names(r_ecdf) <- names(r_industry) <- 1:length(r_industry)
    res = resall # Default
    
    if (!zero_special){
      r_scored = scrub(r_scored)
      r_ecdf = scrub(r_ecdf)
      if (sum(r_ecdf !=0)) {   # Note: we don't use both NAs and 0s
        # Do it by industry
        res = unlist(lapply(unique(r_industry[!is.na(r_industry)]), function(ind) {
          r_ecdfi = r_ecdf[r_industry %in% ind]
          r_scoredi = r_scored[r_industry %in% ind]
          resi = resall[r_industry %in% ind]
          if (sum(r_ecdfi != 0)){
            score_fun = ecdf(r_ecdfi[r_ecdfi!= 0])
            resi = ifelse(r_scoredi!=0, score_fun(r_scoredi),NA)
          }
          names(resi) <- names(r_scoredi)
          resi
        }))
        # add the NA industries now
        tmp = rep(NA,sum(is.na(r_industry)))
        names(tmp) <- names(r_industry[is.na(r_industry)])
        res = c(res,tmp)
        # Just re-order now
        res = res[names(r_scored)]
      }
    } else {
      if (sum(!is.na(r_ecdf))) {   # Note: we don't use NAs only here
        # Do it by industry
        res = unlist(lapply(unique(r_industry[!is.na(r_industry)]), function(ind) {
          r_ecdfi = r_ecdf[r_industry %in% ind]
          r_scoredi = r_scored[r_industry %in% ind]
          resi = resall[r_industry %in% ind]
          if (sum(!is.na(r_ecdfi))){
            score_fun = ecdf(r_ecdfi[!is.na(r_ecdfi)])
            resi = ifelse(!is.na(r_scoredi), score_fun(r_scoredi),NA)
          }
          names(resi) <- names(r_scoredi)
          resi
        }))
        # add the NA industries now
        tmp = rep(NA,sum(is.na(r_industry)))
        names(tmp) <- names(r_industry[is.na(r_industry)])
        res = c(res,tmp)
        # Just re-order now
        res = res[names(r_scored)]
      }
    }
    res
  }))
  rownames(tmp) <- rownames(company_feature_matrix)
  colnames(tmp) <- colnames(company_feature_matrix)
  tmp
}
