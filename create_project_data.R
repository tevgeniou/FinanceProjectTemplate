
# Creating the raw dataset for the project

##########################################################################################
# NOTE: THIS FILE REQUIRES ACCESS TO DATA FROM WRDS. OTHERWISE IT CANNOT BE USED
##########################################################################################

# This is Windows-specific
# if (memory.limit()<40*1024) memory.limit(40*1024) # allow at least 40GB of memory to be allocated (Windows)

rm(list=ls()) # Clean up the memory, if we want to rerun from scratch

###################################################################################################
source("helpers/lib_helpers.R", chdir=TRUE)
source("helpers/latex_code.R")
source("helpers/wrds_helpers.R", chdir=TRUE)
source("helpers/ff_industries_sic.R")
###################################################################################################
source("Paper_global_parameters.R")
###################################################################################################

################################################################################
##### NOTE: HERE WE NEED TO USE A WRDS USERNAME AND PASSWORD, AND ALSO HAVE OTHER DATA FROM WRDS
################################################################################


# These are not publicly available. Search also below for other data files starting with "../FinanceData..."
# UNCOMMENT THE FILES NEEDED (or add other ones from ../FinanceData)
load("../FinanceData/created_monthly_data/GLOBAL_MONTHLY_DATABASE.Rdata")
load("../FinanceData/created_yearly_data/GLOBAL_YEARLY_DATABASE.Rdata")
load("../FinanceData/created_yearly_data/ALL_COMPUSTAT_DATA.Rdata")
#load("../FinanceData/created_ibes_data/GLOBAL_IBES_DATABASE.Rdata")
#load("../FinanceData/created_buyback_data/GLOBAL_BUYBACK.Rdata")
#load("../FinanceData/created_issuers_data/GLOBAL_ISSUERS.Rdata")
#load("../FinanceData/created_daily_data/GLOBAL_DAILY_DATABASE.Rdata")
# We don't need these, so for memory purpose we remove 
#GLOBAL_DAILY_DATABASE$volume_daily <- NULL
#GLOBAL_DAILY_DATABASE$recent_volatility_daily <- NULL
#GLOBAL_DAILY_DATABASE$FamaFrench_five_factors <- NULL
load("../FinanceData/created_projects_datasets/BUYBACKS.Rdata")

if (0){ # USE WRDS
  
  # Uncomment in case wrds is used directly
  # Used to get WRDS data through the API
  source("helpers/wrds_helpers.R", chdir=TRUE)
  source("FILELOCATION/startWRDSconnection.R") # This file has the username pasword for WRDS. These lines below. See wrds_config.R in helpers
  # wrds_user <- "my_username"
  # wrds_pass <- "{SAS002}DBCC5712369DE1C65B19864C1564FB850F398DCF"
  # wrds_path <- "C:\\Users\\my_user\\Documents\\WRDS_Drivers\\"
  wrds_handle <- wrdsConnect()
  
  load_compustat_data_from_wrds <- function(compustat_vars, start_date=as.Date("1980-01-01")) {
    conn = wrdsConnect()
    start_date_sas_format = paste0("'", format(start_date, "%d%b%Y"), "'d")
    vars = paste0("comp.", compustat_vars, collapse=",")
    
    query = paste0(
      "select 
      comp.datadate, 
      comp.GVKEY,", 
      vars,
      ",crsp.LPERMNO 
    from 
      COMP.FUNDA as comp
      left join CRSP.CCMXPF_LNKHIST as crsp
      on (crsp.GVKEY = comp.GVKEY
          and ( 
                (crsp.LINKDT <= comp.datadate <= crsp.LINKENDDT) or 
                (crsp.LINKENDDT is NULL and crsp.LINKDT <= comp.datadate)
              )
         )
    where
      comp.datadate >= ", start_date_sas_format,
      "and crsp.LPERMNO is not NULL
      and crsp.LINKTYPE in ('LC', 'LS', 'LU')
      and crsp.LINKPRIM in ('C', 'P')")
    
    res = data.table::as.data.table(dbGetQuery(conn, query))
    res$datadate = as.Date(res$datadate)
    
    # Order duplicate datadate-LPERMNO combinations by number of NAs per row
    # Keep the row with the fewest NAs (i.e. the first)
    res[, nas_per_row := rowSums(is.na(res))]
    setkeyv(res, c("datadate", "LPERMNO", "nas_per_row"))
    res = res[unique(res[,.(datadate, LPERMNO)]), mult="first"]
    res[, nas_per_row := NULL]
    
    wrdsDisconnect(conn)
    return(res)
  }
  
  compustat_vars = c("at", "xrd")
  all_compustat_data = load_compustat_data_from_wrds(compustat_vars, as.Date("1980-01-01"))
  # Keep only up to 2015-09-30, because it needs to be aligned with the other datasets
  all_compustat_data = all_compustat_data[datadate <= as.Date("2015-09-30")]
}

##########################################################################################
# Now create some new months X permno firm characteristics:

feature_now = all_compustat_data$xrd/all_compustat_data$at
# Add now in the database
all_compustat_data$feature_now = feature_now
GLOBAL_YEARLY_DATABASE$RnD = create_yearly_data("feature_now", GLOBAL_MONTHLY_DATABASE$returns_monthly,all_compustat_data)
rm("feature_now")

##########################################################################################
# THEN WE ADD THE NEW VARIABLES IN THE BUYBACK DATASET WE HAVE:
# A helper function that given the event dates it generates the new features. date_lag_function defines the month relative to the event to get the data from
tmp_names = sapply(1:ncol(GLOBAL_MONTHLY_DATABASE$returns_monthly), function(i) paste(str_sub(rownames(GLOBAL_MONTHLY_DATABASE$returns_monthly), start = 1, end = 7), colnames(GLOBAL_MONTHLY_DATABASE$returns_monthly)[i]))
create_event_feature <- function(event.permnos, monthly.feature.matrix, event_date){
  monthly.feature.matrix = as.vector(monthly.feature.matrix)
  names(monthly.feature.matrix) = tmp_names
  used = paste(event_date,as.character(event.permnos))
  monthly.feature.matrix[used]
}

# Let's add some variables (e.g. 1 and 2 years ago)
time_used = sapply(BUYBACK_DATA$DATASET$SDC$Event.Date, function(x) str_sub(as.character(AddMonths(as.Date(x),-12)), start = 1, end = 7))
BUYBACK_DATA$RnDLastYear = create_event_feature(BUYBACK_DATA$DATASET$SDC$permno, GLOBAL_YEARLY_DATABASE$RnD,time_used)

time_used = sapply(BUYBACK_DATA$DATASET$SDC$Event.Date, function(x) str_sub(as.character(AddMonths(as.Date(x),-24)), start = 1, end = 7))
BUYBACK_DATA$RnDLast2Year = create_event_feature(BUYBACK_DATA$DATASET$SDC$permno, GLOBAL_YEARLY_DATABASE$RnD,time_used)

BUYBACK_DATA$RD =  BUYBACK_DATA$RnDLastYear + BUYBACK_DATA$RnDLast2Year

## Add some cross-sectional score
# THIS IS FOR THE CROSS-SECTIONAL SCORE:
GLOBAL_YEARLY_DATABASE$RnD_score = get_cross_section_score(GLOBAL_YEARLY_DATABASE$RnD)

time_used = sapply(BUYBACK_DATA$DATASET$SDC$Event.Date, function(x) str_sub(as.character(AddMonths(as.Date(x),-12)), start = 1, end = 7))
BUYBACK_DATA$RnDLastYear_score = create_event_feature(BUYBACK_DATA$DATASET$SDC$permno, GLOBAL_YEARLY_DATABASE$RnD_score,time_used)

time_used = sapply(BUYBACK_DATA$DATASET$SDC$Event.Date, function(x) str_sub(as.character(AddMonths(as.Date(x),-24)), start = 1, end = 7))
BUYBACK_DATA$RnDLast2Year_score = create_event_feature(BUYBACK_DATA$DATASET$SDC$permno, GLOBAL_YEARLY_DATABASE$RnD_score,time_used)

BUYBACK_DATA$RD_score = (BUYBACK_DATA$RnDLastYear_score + BUYBACK_DATA$RnDLast2Year_score)/2


##########################################################################################
# Finally save the main data structure
save(BUYBACK_DATA, file = "DemoProject.Rdata")

