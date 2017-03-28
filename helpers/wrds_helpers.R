#  Copyright 2016, INSEAD
#  by V. Kapartzianis 
#  Dual licensed under the MIT or GPL Version 2 licenses.

# 1. Install Java
# ===============
# Choose 32-bit or 64-bit depending on whether you're using 32-bit or 64-bit R/RStudio
# http://www.java.com/en/download/manual.jsp
# 
# If that doesn’t work, you could also manually set the directory of your Java location by setting it before loading the library:
# 
# Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre7') # for 64-bit version
# Sys.setenv(JAVA_HOME='C:\\Program Files (x86)\\Java\\jre7') # for 32-bit version
#
# 2. Install R packages rJava and RJDBC
# =====================================
# You can do that in Rstudio by calling install.packages()
#
# install.packages(c("rJava", "RJDBC"))
#
# 3. Configure your WRDS Cloud access
# ===================================
# For detailed instructions, see wrds_config.R


library(rJava)
.jinit(parameters="-Xmx8g")
library(RJDBC)
source("wrds_config.R")
library(xts)
library(lubridate)

## **** SEE ALSO 
## https://wrds-web.wharton.upenn.edu/wrds/support/Accessing%20and%20Manipulating%20the%20Data/_007R%20Programming/_001Using%20R%20with%20WRDS.cfm
## and ~/.Rprofile (in home directory of local computer)
wrdsConnect <- function(user=wrds_user, pass=wrds_pass) {
  sas_core <- paste0(wrds_path, "sas.core.jar")
  sas_javatools <- paste0(wrds_path, "sas.intrnet.javatools.jar")
  .jinit()
  .jaddClassPath(c(sas_core, sas_javatools))
  driver <- JDBC("com.sas.net.sharenet.ShareNetDriver", sas_javatools, identifier.quote="`")
  wrds_handle <- dbConnect(driver, "jdbc:sharenet://wrds-cloud.wharton.upenn.edu:8551/", user, pass)
  return(wrds_handle)
}

wrdsDisconnect <- function(wrds_handle) {
  invisible(dbDisconnect(wrds_handle))
}

# 4. Sample usage
# ===============
# > wrds_handle <- wrdsConnect()
# > res <- dbSendQuery(wrds_handle, "select distinct libname from dictionary.tables")
# > data <- fetch(res, n = -1)
# > print(head(data))
# libname
# 1 AUDIT   
# 2 BANK    
# 3 BLOCK   
# 4 BOARDEX 
# 5 BVD     
# 6 BVDSAMP 
# > wrdsDisconnect(wrds_handle)

wrdsQueryMarketIndex <- function(wrds_handle, field, start=as.Date("1980-01-01"), periodicity=c("daily","monthly")) {
  FIELD <- tolower(paste(field, collapse=", "))
  PCHAR <- substring(toupper(periodicity[1]), 1, 1)
  FDATE <- paste0("'", format(start, "%d%b%Y"), "'d")
  query <- paste0('
  select DATE, ',FIELD,'
    from CRSP.',PCHAR,'SI
    where DATE >= ',FDATE,'
  ;')
  data <- dbGetQuery(wrds_handle, query)
  xts(data[, tolower(field), drop=FALSE], as.Date(data[, "DATE"]))
}

# retrieve multiple fields for one PERMNO
wrdsQueryStockField <- function(wrds_handle, PERMNO, field, start=as.Date("1980-01-01"), periodicity=c("daily","monthly")) {
  FIELD <- toupper(paste(field, collapse=", "))
  PCHAR <- substring(toupper(periodicity[1]), 1, 1)
  FDATE <- paste0("'", format(start, "%d%b%Y"), "'d")
  query <- paste0('
  select DATE, ',FIELD,'
    from CRSP.',PCHAR,'SF
    where PERMNO = ',PERMNO,'
      and DATE >= ',FDATE,'
  ;')
  data <- dbGetQuery(wrds_handle, query)
  xts(data[, toupper(field), drop=FALSE], as.Date(data[, "DATE"]))
}

# retrieve a single field for multiple PERMNOs
wrdsQueryStockFieldMatrix <- function(wrds_handle, PERMNO, field, start=as.Date("1980-01-01"), periodicity=c("daily","monthly")) {
  FIELD <- toupper(paste(field, collapse=", "))
  PCHAR <- substring(toupper(periodicity[1]), 1, 1)
  FDATE <- paste0("'", format(start, "%d%b%Y"), "'d")
  UPERMNO <- unique(PERMNO)
  INPUT <- data.frame(UPERMNO)
  chunk <- 1000/2 # split inputs in chunks, to avoid large query stacks
  INPUT <- split(INPUT, rep(1:ceiling(nrow(INPUT)/chunk), each=chunk)[1:nrow(INPUT)])
  data <- do.call(rbind, lapply(INPUT, function(X) {
    query <- paste0('
    select PERMNO, DATE, ',FIELD,'
      from CRSP.',PCHAR,'SF
      where PERMNO in (',paste(X$UPERMNO, collapse=", "),')
        and DATE >= ',FDATE,'
    ;')
    dbGetQuery(wrds_handle, query)
  }))
  data <- data.table::as.data.table(data)
  data <- data.table::dcast(data, DATE ~ PERMNO, value.var = toupper(field), fill=NA)
  xts(data[, 2:ncol(data), with=FALSE], as.Date(data$DATE))
}

# retrieve multiple fields for one GVKEY
wrdsQueryPensionField <- function(wrds_handle, gvkey, field, start=as.Date("1980-01-01"), periodicity=c("annual","quarterly")) {
  FIELD <- tolower(paste(field, collapse=", "))
  PCHAR <- substring(toupper(periodicity[1]), 1, 1)
  FDATE <- paste0("'", format(start, "%d%b%Y"), "'d")
  query <- paste0('
  select datadate, ',FIELD,'
    from COMPM.ACO_PNFND',PCHAR,'
    where consol = "C"
      and indfmt = "INDL"
      and datafmt = "STD"
      and popsrc = "D"
      and gvkey = "',gvkey,'"
      and datadate >= ',FDATE,'
  ;')
  data <- dbGetQuery(wrds_handle, query)
  xts(data[, tolower(field), drop=FALSE], as.Date(data[, "datadate"]))
}

# retrieve a single field for multiple GVKEYs
wrdsQueryPensionFieldMatrix <- function(wrds_handle, gvkey, field, start=as.Date("1980-01-01"), periodicity=c("annual","quarterly")) {
  FIELD <- tolower(paste(field, collapse=", "))
  PCHAR <- substring(toupper(periodicity[1]), 1, 1)
  FDATE <- paste0("'", format(start, "%d%b%Y"), "'d")
  UGVKEY <- unique(gvkey)
  INPUT <- data.frame(UGVKEY)
  chunk <- 1000/2 # split inputs in chunks, to avoid large query stacks
  INPUT <- split(INPUT, rep(1:ceiling(nrow(INPUT)/chunk), each=chunk)[1:nrow(INPUT)])
  data <- do.call(rbind, lapply(INPUT, function(X) {
    query <- paste0('
    select gvkey, datadate, ',FIELD,'
      from COMPM.ACO_PNFND',PCHAR,'
      where consol = "C"
        and indfmt = "INDL"
        and datafmt = "STD"
        and popsrc = "D"
        and gvkey in ("',paste(X$UGVKEY, collapse="\", \""),'")
        and datadate >= ',FDATE,'
    ;')
    dbGetQuery(wrds_handle, query)
  }))
  data <- data.table::as.data.table(data)
  data <- data.table::dcast(data, datadate ~ gvkey, value.var = tolower(field), fill=NA)
  xts(data[, 2:ncol(data), with=FALSE], as.Date(data$datadate))
}

wrdsQueryEstimatesField <- function(wrds_handle, table, PERMNO, field, start=as.Date("1980-01-01")) {
  # list of tables at https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=40
  TABLE <- toupper(table)
  FIELD <- toupper(paste(paste0("estimates.", field), collapse=", "))
  FDATE <- paste0("'", format(start, "%d%b%Y"), "'d")
  query <- paste0('
  select ',FIELD,'
    from CRSP.DSENAMES as names, IBES.',TABLE,' as estimates
    where names.CUSIP = estimates.CUSIP
      and names.PERMNO in (',PERMNO,')
  ;')
  data <- dbGetQuery(wrds_handle, query)
}

wrdsLookupCUSIP <- function(wrds_handle, CUSIP, date) {
  FDATE <- paste0("'", format(date, "%d%b%Y"), "'d")
  query <- paste0('
  select securities.TIC, securities.SCUSIP, identifiers.LPERMNO, securities.GVKEY, securities.ISIN, securities.SEDOL
    from CRSP.CCMXPF_LNKHIST identifiers, CRSP.SECHEAD as securities 
    where identifiers.GVKEY = securities.GVKEY
    and identifiers.LPERMNO is not NULL
    and identifiers.LINKTYPE in ("LC", "LS", "LU")
    and (', paste(paste0(
      '((identifiers.LINKDT <= ',FDATE,' <= identifiers.LINKENDDT
        or identifiers.LINKENDDT is NULL and identifiers.LINKDT <=',FDATE,')
      and securities.SCUSIP like "',CUSIP,'%")'), collapse=" or "), ')
  ;')
  dbGetQuery(wrds_handle, query)
}

wrdsGetPERMNOForCUSIP <- function(wrds_handle, CUSIP, date) {
  FDATE <- paste0("'", format(date, "%d%b%Y"), "'d")
  INPUT <- data.frame(CUSIP, FDATE)
  chunk <- 1000 # split inputs in chunks, to avoid large query stacks
  INPUT <- split(INPUT, rep(1:ceiling(nrow(INPUT)/chunk), each=chunk)[1:nrow(INPUT)])
  wdata <- do.call(rbind, lapply(INPUT, function(X) {
    query <- paste0('
    select TICKER, PERMNO, NCUSIP, NAMEDT, NAMEENDT
      from CRSP.DSENAMES
      where (', paste(paste0(
        '((NAMEDT <= ',X$FDATE,' <= NAMEENDT
          or NAMEENDT is NULL and NAMEDT <=',X$FDATE,')
        and NCUSIP like "',X$CUSIP,'%")'), collapse=" or "), ')
    ;')
    dbGetQuery(wrds_handle, query)
  }))
  mapply(function(CUSIP, date) {  # TODO: loop probably needs optimization
    matched.rows <- unique(wdata[substr(wdata$NCUSIP, 1, nchar(CUSIP)) == CUSIP, ])
    matched.rows$NAMEENDT[is.na(matched.rows$NAMEENDT)] <- as.character(Sys.Date())
    permno.index <- which(as.Date(matched.rows$NAMEDT) <= date & date <= as.Date(matched.rows$NAMEENDT))
    as.numeric(if (length(permno.index) == 1) matched.rows$PERMNO[permno.index] else NA)
  }, CUSIP, date)
}

wrdsGetPERMNOForGVKEY <- function(wrds_handle, GVKEY, date, allow.multiple=F) {
  FDATE <- paste0("'", format(date, "%d%b%Y"), "'d")
  INPUT <- data.frame(GVKEY, FDATE)
  chunk <- 1000 # split inputs in chunks, to avoid large query stacks
  INPUT <- split(INPUT, rep(1:ceiling(nrow(INPUT)/chunk), each=chunk)[1:nrow(INPUT)])
  wdata <- do.call(rbind, lapply(INPUT, function(X) {
    query <- paste0('
                    select GVKEY, LPERMNO, LINKDT, LINKENDDT
                    from CRSP.CCMXPF_LNKHIST
                    where LPERMNO is not NULL
                    and LINKTYPE in ("LC", "LS", "LU")
                    and LINKPRIM in ("C", "P")
                    and (', paste(paste0(
                      '((LINKDT <= ',X$FDATE,' <= LINKENDDT
                      or LINKENDDT is NULL and LINKDT <=',X$FDATE,')
                      and GVKEY = "',X$GVKEY,'")'), collapse=" or "), ')
                    ;')
    dbGetQuery(wrds_handle, query)
  }))
  mapply(function(GVKEY, date) {  # TODO: loop probably needs optimization
    matched.rows <- unique(wdata[wdata$GVKEY == GVKEY, ])
    matched.rows$LINKENDDT[is.na(matched.rows$LINKENDDT)] <- as.character(Sys.Date())
    permno.index <- which(as.Date(matched.rows$LINKDT) <= date & date <= as.Date(matched.rows$LINKENDDT))
    max.permnos <- if (allow.multiple) .Machine$integer.max else 1
    as.numeric(if (length(permno.index) > 0) head(matched.rows$LPERMNO[permno.index], max.permnos) else NA)
  }, GVKEY, date)
}

wrdsGetGVKEYForPERMNO <- function(wrds_handle, PERMNO, date) {
  FDATE <- paste0("'", format(date, "%d%b%Y"), "'d")
  INPUT <- data.frame(PERMNO, FDATE)
  chunk <- 1000 # split inputs in chunks, to avoid large query stacks
  INPUT <- split(INPUT, rep(1:ceiling(nrow(INPUT)/chunk), each=chunk)[1:nrow(INPUT)])
  wdata <- do.call(rbind, lapply(INPUT, function(X) {
    query <- paste0('
    select GVKEY, LPERMNO, LINKDT, LINKENDDT
      from CRSP.CCMXPF_LNKHIST
      where LPERMNO is not NULL
        and LINKTYPE in ("LC", "LS", "LU")
        and (', paste(paste0(
          '((LINKDT <= ',X$FDATE,' <= LINKENDDT
            or LINKENDDT is NULL and LINKDT <=',X$FDATE,')
          and LPERMNO = ',X$PERMNO,')'), collapse=" or "), ')
    ;')
    dbGetQuery(wrds_handle, query)
  }))
  mapply(function(PERMNO, date) {  # TODO: loop probably needs optimization
    matched.rows <- unique(wdata[wdata$LPERMNO ==PERMNO, ])
    matched.rows$LINKENDDT[is.na(matched.rows$LINKENDDT)] <- as.character(Sys.Date())
    gvkey.index <- which(as.Date(matched.rows$LINKDT) <= date & date <= as.Date(matched.rows$LINKENDDT))
    as.character(if (length(gvkey.index) == 1) matched.rows$GVKEY[gvkey.index] else NA)
  }, PERMNO, date)
}


wrdsGetPERMCOForCUSIP <- function(wrds_handle, CUSIP, date) {
  FDATE <- paste0("'", format(date, "%d%b%Y"), "'d")
  INPUT <- data.frame(CUSIP, FDATE)
  chunk <- 1000 # split inputs in chunks, to avoid large query stacks
  INPUT <- split(INPUT, rep(1:ceiling(nrow(INPUT)/chunk), each=chunk)[1:nrow(INPUT)])
  wdata <- do.call(rbind, lapply(INPUT, function(X) {
    query <- paste0('
                    select TICKER, PERMCO, NCUSIP, NAMEDT, NAMEENDT
                    from CRSP.DSENAMES
                    where (', paste(paste0(
                      '((NAMEDT <= ',X$FDATE,' <= NAMEENDT
                      or NAMEENDT is NULL and NAMEDT <=',X$FDATE,')
                      and NCUSIP like "',X$CUSIP,'%")'), collapse=" or "), ')
                    ;')
    dbGetQuery(wrds_handle, query)
  }))
  mapply(function(CUSIP, date) {  # TODO: loop probably needs optimization
    matched.rows <- unique(wdata[substr(wdata$NCUSIP, 1, nchar(CUSIP)) == CUSIP, ])
    matched.rows$NAMEENDT[is.na(matched.rows$NAMEENDT)] <- as.character(Sys.Date())
    permco.index <- which(as.Date(matched.rows$NAMEDT) <= date & date <= as.Date(matched.rows$NAMEENDT))
    as.numeric(if (length(permco.index) == 1) matched.rows$PERMCO[permco.index] else NA)
  }, CUSIP, date)
}

wrdsGetMarketCap <- function(wrds_handle, PERMNO, date, periodicity=c("monthly", "yearly", "BE.ME")) {
  pdate <- switch(periodicity[1], 
                 monthly=as.Date(format(date, "%Y-%m-01")) - 1,
                 yearly =as.Date(format(date, "%Y-01-01")) - 1,
                 BE.ME  =as.Date(format(date, "%Y-01-01")) - 1 - years(month(date) <= 6))
  FDATE <- format(pdate, "%Y-%m")
  INPUT <- unique(data.frame(PERMNO, FDATE))
  chunk <- 255 # split inputs in chunks, to avoid hitting the UNION ALL limit
  INPUT <- split(INPUT, rep(1:ceiling(nrow(INPUT)/chunk), each=chunk)[1:nrow(INPUT)])
  wdata <- do.call(rbind, lapply(INPUT, function(X) {
    query <- paste0('
    select PERMNO, put(DATE, yymmd7.) as FDATE, SHROUT * PRC as MKTCAP
      from CRSP.MSF
      where put(DATE, yymmd7.) = \'',X$FDATE,'\' and PERMNO = ',X$PERMNO,'
    ', collapse=" UNION ALL")
    data.table::data.table(dbGetQuery(wrds_handle, query))
  }))
  data.table::setkey(wdata, "PERMNO", "FDATE")
  wdata[.(get("PERMNO", envir=parent.env(environment())), get("FDATE", envir=parent.env(environment()))), MKTCAP, mult="first"]
}

# Querying date ranges (start <= date <= end) is slow, so we use IN with distinct values
# TODO: return NA if number of days for AVGPRC is less then a provided threshold
wrdsGetAveragePrice <- function(wrds_handle, PERMNO, start, end) {
  FDATE <- mapply(function(start, end) paste0("'", format(start + 0:(end-start), "%d%b%Y"), "'d", collapse=" , "), start, end)
  INPUT <- data.frame(PERMNO, FDATE, start, end)
  chunk <- 255 # split inputs in chunks, to avoid hitting the UNION ALL limit
  INPUT <- split(INPUT, rep(1:ceiling(nrow(INPUT)/chunk), each=chunk)[1:nrow(INPUT)])
  wdata <- do.call(rbind, lapply(INPUT, function(X) {
    query <- paste0('
    select PERMNO, AVG(ABS(PRC)) as AVGPRC, "',X$start,'" as START, "',X$end,'" as END
      from CRSP.DSF
      where PERMNO = ',X$PERMNO,' and DATE IN (',X$FDATE,')
      GROUP BY PERMNO
    ', collapse=" UNION ALL")
    data.table::data.table(dbGetQuery(wrds_handle, query))
  }))
  data.table::setkey(wdata, "PERMNO", "START", "END")
  wdata[.(get("PERMNO", envir=parent.env(environment())), as.character(start), as.character(end)), AVGPRC, mult="first"]
}

# Fetch Compustat indices, the whole table if `name` isn't specified
# Table at https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=130&file_id=66165
wrdsQueryIndices <- function(wrds_handle, name="") {
  query <- paste0('
                  select *
                  from COMPM.IDX_INDEX
                  where conm like "%', name, '%"
                  ;')
  dbGetQuery(wrds_handle, query)
}

# Get index constituents (GVKEYs) for indices on specific dates
# Table at https://wrds-web.wharton.upenn.edu/wrds//tools/variable.cfm?library_id=130&file_id=66159
wrdsGetIndexConstituents <- function(wrds_handle, GVKEYX, date=Sys.Date()) {
  FDATE <- paste0("'", format(date, "%d%b%Y"), "'d")
  INPUT <- data.frame(GVKEYX, FDATE)
  chunk <- 1000 # split inputs in chunks, to avoid large query stacks
  INPUT <- split(INPUT, rep(1:ceiling(nrow(INPUT)/chunk), each=chunk)[1:nrow(INPUT)])
  wdata <- do.call(rbind, lapply(INPUT, function(X) {
    query <- paste0('
                    select GVKEY, GVKEYX, from, thru
                    from COMPM.IDXCST_HIS
                    where (', paste(paste0(
                      '((from <= ',X$FDATE,' <= thru
                      or thru is NULL and from <=',X$FDATE,')
                      and GVKEYX = "',X$GVKEYX,'")'), collapse=" or "), ')
                    ;')
    dbGetQuery(wrds_handle, query)
  }))
  mapply(function(GVKEYX, date) {  # TODO: loop probably needs optimization
    matched.rows <- unique(wdata[wdata$gvkeyx == GVKEYX, ])
    matched.rows$thru[is.na(matched.rows$thru)] <- as.character(Sys.Date())
    gvkey.index <- which(as.Date(matched.rows$from) <= date & date <= as.Date(matched.rows$thru))
    as.character(if (length(gvkey.index) > 0) matched.rows$gvkey[gvkey.index] else NA)
  }, GVKEYX, date, SIMPLIFY=FALSE)
}

# Get S&P 500 index constituents (PERMNOs) on specific dates
# Table at https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=138&file_id=67168
wrdsGetSP500Constituents <- function(wrds_handle, date=as.Date(paste(year(Sys.Date())-1, 12, 30, sep="-"))) {
  FDATE <- paste0("'", format(date, "%d%b%Y"), "'d")
  query <- paste0('
                  select PERMNO
                  from CRSP.DSP500LIST
                  where (start <= ',FDATE,' <= ending)
                  or (ending is NULL and start <=',FDATE,')
                  ;')
  dbGetQuery(wrds_handle, query)$PERMNO
}
