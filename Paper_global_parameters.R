
############################################################################################ 
# All global parameters of the project
############################################################################################ 
use_major_market_score = 0 # We use all CRSP universe for the scores 

remove_financials_utilities = 1 # Remove the financials and utilities? (Default is 1)
remove_missing_permnosV2 = 1 # remove also the permnos that are missing when they are found using the NCUSIP-Date? (Default is 1, to avoid any data issues) 
remove_CRSP_minor_markets = 0 # remove also the non-major markets according to CRSP? (Default is 0 as this is not historic exchange)

continuous_valuation_index = 0 # Use discrete or continuous scoring for the U-Index (Default is 0)
do.value.weight = 0 # Do we value weight for the calendar method? (Default is 0)

##########################################################################################
First = as.Date("1976-01-01")
Last = as.Date("2015-12-31")
# THIS IS THE FIRST DAY WE TRADE ANY PORTFOLIOS IN THE PORTFOLIOS SECTION: 
FirstTrade = as.Date("1985-01-01")
periods_considered = rbind(
  c("1985-01-01", "1990-12-31"),
  c("1991-01-01", "2000-12-31"),
  c("2001-01-01", "2015-12-31"),
  c("2008-01-01", "2015-12-31")
)

penny_stock_price_recent = 3 # penny stock threshold price - post 1990
penny_stock_price_old = 1 # penny stock threshold price - pre 1990
good_currencies = c("US")
major_markets = c("N", "NM", "NY", "NK",      "New York", "Nasdaq", "NYSE Amex", "American")
major_markets_crsp = c(11,12,14)
MIN_SIZE = 0 #micro caps are those with a market cap <300m
MAX_SIZE = 1e20 #already in million (e6)
MAX_SIZE_SCORE = 1e20
MIN_EVENT_SIZE = 0 # Required minimum "event size" (e.g. % of shares repurchased/issued)
MAX_EVENT_SIZE = 50 # not allow more than this percent of shares for the event
CROSS_EVENTS_SINCE_LAST_EVENT = 4*365 # we require that there were at most that many days before the previous event
minCROSS_EVENTS_SINCE_LAST_EVENT = 0 # we require that there were at least that many days before the previous event

BB_allowed_techniques = c("OP", "OPNG","") # Which Buybacks techniques are allowed (note that "" is needed to keep those for 
# which we do not have the technique - e.g. all buyback events from the SDC M&A database)
#       OP OPDA OPNG OPOL 
ISS_allowed_techniques = c("FIRMCOMMITMENT+NEGOTIATEDSALE", "NEGOTIATEDSALE+FIRMCOMMITMENT",
                           "FIRMCOMMITMENT", "AUCTION+NEGOTIATEDSALE", "NEGOTIATEDSALE",
                           "FIRMCOMMITMENT+FIRMCOMMITMENT+NEGOTIATEDSALE",
                           "PLACEMENT","FIRMCOMMITMENT+AUCTION",
                           "FIRMCOMMITMENT+NEGOTIATEDSALE+OFFERSAL",
                           "NEGOTIATEDSALE+OFFERSALE+FIRMCOMMITMENT"
) # Which SEO techniques are allowed 

##############################################################################
# U-index parameters

quantile_simple = 0.2
quantile_Uindex <- quantile_R2 <- quantile_VOL <- quantile_LEV <- quantile_EPS <- quantile_simple # Quantile for defining high/low U-index based on the U-index score
years_across_cross_events = 4 # for BB then ISS or vice verca years.

##########################################################################################
# Industries and industry filter

All = 1:9999
Finance1 = 6000:6799
# Select the industries to use
Industries = All
INDUSTRY_USED = setdiff(All,union(union(FF_industries$Trading, FF_industries$Banking),FF_industries$Utilities)) # what industry to check in the sector sensitivity part of the paper (only used there)
INDUSTRY_FINANCIALS = union(FF_industries$Trading, FF_industries$Banking) # what industry to check in the sector sensitivity part of the paper (only used there)

min_industry_sampleBB = 300

##########################################################################################
# Risk Factors

pnl_hedge_factors = c("RF","Delta","SMB","HML","RMW","CMA") # for the "strategy" hedge

##########################################################################################
# Reported months

reported_times = c("-6","+12","+24","+36","+48","Observations")
reported_times_post= c("+12","+24","+36","+48","Observations")
allmonths = (-6):48
reported_times_plots = ifelse(allmonths > 0, paste("+",allmonths,sep=""), allmonths)
plot_tickers= c(which(reported_times_plots=="-6"),which(reported_times_plots=="0"),which(reported_times_plots=="+12"),which(reported_times_plots=="+24"),which(reported_times_plots=="+36"),which(reported_times_plots=="+48"))
plot_xlabels = c("-6","0","+12","+24","+36","+48")

##########################################################################################
# Parameters for trading and P&L performances

trading_day_after_announce = 1 
start_date_event = "Trading.Day" # see above. this needs some cleanup as we don't need all these dates any more. but ok for now, as it is not slow to do is as is. keep it as is for future options
hedge_days = 250
hedge_months = 18

##########################################################################################
RECESSION_YEARS = list( #http://www.nber.org/cycles.html
  nineties = as.Date(c("1990-07-01", "1991-03-01")),
  dotcom = as.Date(c("2001-03-01", "2001-11-01")),
  financial = as.Date(c("2007-12-01", "2009-07-01")))

BEAR_YEARS = list( #http://www.gold-eagle.com/article/history-us-bear-bull-markets-1929
  dotcom = as.Date(c("2000-03-24", "2002-10-09")),
  financial1 = as.Date(c("2007-10-09", "2009-03-09"))
)

BEAR_YEARS_text = list( #http://www.gold-eagle.com/article/history-us-bear-bull-markets-1929
  dotcom = c("March 2000", "October 2002"),
  financial1 = c("October 2007", "March 2009")
)

do_Stambaugh_calculation = FALSE
use_mispricing_factors = TRUE
use_q_factors = FALSE

