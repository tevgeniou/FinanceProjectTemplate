
##########################################################################################
##########################################################################################
# Filter the data used for the paper
##########################################################################################
##########################################################################################

##########################################################################################
# Helper functions
##########################################################################################

# dataset_name can be either "buybacks" or "issuers"
get_data <- function(dataset_name) {
  if (dataset_name == "buybacks" | dataset_name == "bb") {
    return(BUYBACK_DATA)
  } else if (dataset_name == "issuers" | dataset_name == "iss") {
    return(ISSUERS_DATA)
  } else {
    stop("invalid dataset name")
  }
}

# x can be either BUYBACK_DATA or ISSUERS_DATA 
get_names_for_removal <- function(x) {
  # There is nothing to remove from the market and risk factor data,
  # because it does not refer to specific events
  names_to_exclude = c("Risk_Factors_Monthly", "Market_Monthly", "Market_daily")
  names_for_removal = setdiff(names(x), names_to_exclude)
  # Exclude names that contain "cleanup"
  names_for_removal = names_for_removal[-grep("cleanup", names_for_removal)]
}

# Removes the elements identified by indexes_to_remove from the bottom-level elements of
# a nested list of lists, data frames, matrices, and vectors, which can be arbitrarily 
# nested. Only considers children of the top-level list elements named indicated in 
# names_for_removal.
# Input:
# - x: a list of lists, data frames, matrices, and vectors, arbitrarily nested
# - names_for_removal: a character vector, the top-level elements of x to consider
# - indexes_to_remove: an integer vector, the indexes to remove from bottom-level elements
#   If the bottom-level elements are matrices, it removes rows.
#   If the bottom-level elements are data frames, it removes columns.
#   If the bottom-level elements are lists or vectors, it removes elements.
# Output:
# - A list of the exact same nested structure as the input list x with the elements 
#   removed from nested elements as specified above. 
remove_elements_from_list_recursively <- function(x, names_for_removal, 
                                                  indexes_to_remove) {
  # Case when empty vectors were passed
  if (length(indexes_to_remove) == 0 | length(names_for_removal) == 0) {
    return(x)
  }

  # The R function is.list returns TRUE if the object is a data.frame
  is_list <- function(x) {
    return(is.list(x) & !is.data.frame(x))
  }
  
  all_levels = list()
  levels_to_remove_from <- function(x, levels_names=c()) {
    for (i in seq_along(x)) {
      new_levels_names = c(levels_names, names(x)[i])
      # If it is a list continue the recursion
      # BUT: if the list is unnamed (i.e. only numbered) stop recursing
      if (is_list(x[[i]]) & length(names(x[[i]])) > 0) {
        levels_to_remove_from(x[[i]], new_levels_names)
      } else {
        all_levels[[length(all_levels)+1]] <<- new_levels_names
      }
    }
  }

  tmp = levels_to_remove_from(x[names_for_removal])

  remove_elements_based_on_type <- function(x, indexes_to_remove) {
    if (is.matrix(x)) {
      # Matrix: Remove columns
      return(x[,-indexes_to_remove])
    } else if (is.data.frame(x)) {
      # Data frame: Remove rows
      return(x[-indexes_to_remove,])
    } else {
      # List or vector: Remove elements
      return(x[-indexes_to_remove])
    }
  }

  for (levels_names in all_levels) {
    # The only way to access arbitrary levels of a list of lists is
    # to parse a subsetting string as code
    element_str = paste0("x$", paste(levels_names, collapse="$"))
    element = eval(parse(text=element_str))
    removal_str = paste0(element_str, 
      " = remove_elements_based_on_type(element, indexes_to_remove)")
    eval(parse(text=removal_str))
  }

  return(x)
}

##########################################################################################
# Remove and filter events
##########################################################################################

# dataset_name can be either "buybacks" or "issuers"
remove_and_filter_events <- function(dataset_name) {
  x = get_data(dataset_name)
  names_for_removal = get_names_for_removal(x)

  ########################################################################################
  # Remove events for which we have missing values (for the main missing values)

  indexes_to_remove = which(
    # just in alphabetic order not to forget any    
    is.na(x$BEME_used) | 
    is.na(x$Performance_used) | 
      is.na(x$RD) |
    is.na(x$Size_used) | 
    is.na(x$DATASET$CRSP$pre_vol_Score) |
    is.na(x$DATASET$CRSP$IVOL_score) |
    is.na(x$DATASET$CRSP$Rsq_score) |
    is.na(x$DATASET$CRSP$Market.Cap)
  )
  x = remove_elements_from_list_recursively(x, names_for_removal, indexes_to_remove)
  x$cleanupMissingSomeValues = length(indexes_to_remove)

  ########################################################################################
  # Remove events for which we have missing permnoV2

  indexes_to_remove = which(is.na(x$DATASET$SDC$permnoV2))
  x = remove_elements_from_list_recursively(x, names_for_removal, indexes_to_remove)
  x$cleanupNoPermno = x$cleanupNoPermno + length(indexes_to_remove)


  ########################################################################################
  # Project specific filters

  No_filter = rep(T,length(x$DATASET$SDC$Event.Date))
  # Basic_filter = !is.na(x$DATASET$SDC$ME_quantile) & 
  #                !is.na(x$DATASET$SDC$BEME_quantile)
  Basic_filter =  No_filter  
  Period_filter = (as.Date(x$DATASET$SDC$Event.Date) >= First) & 
                  (as.Date(x$DATASET$SDC$Event.Date) <= Last)
  # Penny_stock_filter = ifelse(
  #   x$DATASET$SDC$Event.Date < "1995-01-01", 
  #   scrub(x$DATASET$SDC$Closing.Price) >= penny_stock_price_old, 
  #   scrub(x$DATASET$SDC$Closing.Price) >= penny_stock_price_recent) 
  Penny_stock_filter = ifelse(x$DATASET$SDC$Event.Date < "1995-01-01", 
                              scrub(x$DATASET$CRSP$closing.price) >= penny_stock_price_old, 
                              scrub(x$DATASET$CRSP$closing.price) >= penny_stock_price_recent) 
  # REMOVE FINANCIALS AND UTILITIES: We leave them for now 
  # so we can use them as needed. This decision is made in bb_issuers_new.R
  # Industry_filter = x$DATASET$SDC$Industry %in% INDUSTRY_USED 
  Industry_filter = 1
  US_only = (x$DATASET$SDC$Currency %in% good_currencies)
  major_markets_only = sapply(x$DATASET$SDC$Stock.Exchange, function(i) {
    return(sum(str_split(i, "\\+")[[1]] %in% major_markets) > 0) 
  })
  
  # dataset-specific filter
  if (dataset_name == "buybacks") {
    Technique_filter = x$DATASET$SDC$Tech..nique.Code %in% BB_allowed_techniques
  } else if (dataset_name == "issuers") {
    Technique_filter = x$DATASET$SDC$Offering.Technique %in% ISS_allowed_techniques
  }

  TOTAL_FILTER_basic = No_filter & Basic_filter & Period_filter & 
                       Penny_stock_filter & Industry_filter & US_only & 
                       major_markets_only & Technique_filter
  # Market_cap_filter = (scrub(x$DATASET$SDC$Market.Cap) >= MIN_SIZE) & 
  #                     (scrub(x$DATASET$SDC$Market.Cap) <= MAX_SIZE)
  Market_cap_filter = (scrub(x$DATASET$CRSP$Market.Cap) >= MIN_SIZE) & 
                      (scrub(x$DATASET$CRSP$Market.Cap) <= MAX_SIZE) & 
                      (scrub(x$DATASET$CRSP$Market.Cap_score) <= MAX_SIZE_SCORE)
  # Leverage_filter = (scrub(x$DATASET$SDC$lt/x$DATASET$SDC$at) > 0.5)*
  #                   (!is.na(x$DATASET$SDC$lt/x$DATASET$SDC$at))
  EventSize_filter = (x$DATASET$SDC$Event.Size >= MIN_EVENT_SIZE) & 
                     (x$DATASET$SDC$Event.Size <= MAX_EVENT_SIZE)
  TOTAL_FILTER_complex = Market_cap_filter & EventSize_filter 
  ### remove
  TOTAL_FILTER = TOTAL_FILTER_basic & TOTAL_FILTER_complex
  BIZ_initial_data = length(TOTAL_FILTER)
  indexes_to_remove = which(!TOTAL_FILTER)
  x = remove_elements_from_list_recursively(x, names_for_removal, indexes_to_remove)

  ### Keep track
  cleanup <- list()
  cleanup$initial_data = BIZ_initial_data
  cleanup$total_removed = length(indexes_to_remove)
  cleanup$Basic_filter = sum(!Basic_filter)
  cleanup$Period_filter = sum(!Period_filter)
  cleanup$Penny_stock_filter = sum(!Penny_stock_filter)
  cleanup$Industry_filter = sum(!Industry_filter)
  cleanup$US_only = sum(!US_only)
  cleanup$BIZ_allowed_techniques = sum(!Technique_filter)
  cleanup$major_markets_only = sum(!major_markets_only)
  cleanup$Market_cap_filter = sum(!Market_cap_filter)
  cleanup$EventSize_filter = sum(!EventSize_filter)

  x$cleanupBIZ = cleanup

  ########################################################################################
  # Remove financials and utilities

  if (remove_financials_utilities) {
    Industry_filter = x$DATASET$SDC$Industry %in% INDUSTRY_USED
    indexes_to_remove = which(!Industry_filter)
    x = remove_elements_from_list_recursively(x, names_for_removal, indexes_to_remove)
    x$cleanupBIZ$Industry_filter = sum(!Industry_filter)
  }

  ########################################################################################
  # Remove CRSP major markets

  if (remove_CRSP_minor_markets) {
    indexes_to_remove = which(!(x$DATASET$CRSP$exchange %in% major_markets_crsp))
    x = remove_elements_from_list_recursively(x, names_for_removal, indexes_to_remove)
    x$cleanupBIZ$major_markets_only = x$cleanupBIZ$major_markets_only + 
                                      length(indexes_to_remove) 
  }

  return(x)
}

BUYBACK_DATA = remove_and_filter_events("buybacks")
ISSUERS_DATA = remove_and_filter_events("issuers")
