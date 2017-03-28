#  Copyright 2016, INSEAD
#  by V. Kapartzianis 
#  Dual licensed under the MIT or GPL Version 2 licenses.


# Fama/French
# ===========
# See http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html

library(xts)

ffRetrieveData <- function(zip.file, section=NULL, field=NULL, start=as.Date("1980-01-01"), skip=NULL) {
  # Retrieves Fama/French timeseries.
  #
  # Args:
  #   zip.file: The source file to process, as provided by Ken French. You can
  #     omit the path and the extension.
  #   section (optional): The section to retrieve from the file. Sources, as
  #     provided by Ken French, can contain different sections, for example the
  #     monthly factors CSV also contains an "Annual Factors: January-December "
  #     section. NOTE: you have to match the section name exactly, and specify
  #     NULL to retrieve the first/only section (which is often un-named).
  #   field (optional): A vector of columns to retrieve. This could be, for
  #     example `c("Banks", "Fin")`.
  #   start (optional): The first date in the timeseries to retrieve. Default is
  #     `as.Date("1980-01-01")`.
  #   skip (optional): The number of lines to skip before processing the file.
  #     NOTE: the function can only skip up to 5 lines automatically, if this
  #     parameter is left to its default value, `NULL`.
  #
  # Returns:
  #   The specified Fama/French timeseries, as an `xts` matrix. NOTE: dimensions
  #   are never dropped in the returned results, the function won't return an
  #   `xts` vector even when a single field is specified or present in the file.
  #
  # Examples:
  #   ffRetrieveData("http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_CSV.zip")
  #   ffRetrieveData("F-F_Research_Data_5_Factors_2x3_CSV", section=" Annual Factors: January-December ")
  #   ffRetrieveData("F-F_Research_Data_5_Factors_2x3_CSV", field="RF")
  #   ffRetrieveData("49_Industry_Portfolios_CSV", field=c("Banks", "Fin"), skip=11)
  
  if (!grepl("http", zip.file)) zip.file <- paste0("http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/", zip.file)
  if (!grepl("zip$", zip.file)) zip.file <- paste0(zip.file,".zip")
  extract.dir <- tempfile()
  dir.create(extract.dir)
  wd <- setwd(extract.dir)
  tryCatch({
    download.file(zip.file, "tmp.zip", quiet=TRUE)
    unzip("tmp.zip")
    csv.file <- setdiff(list.files(), "tmp.zip")
    if (is.null(skip)) for (skip in 1:5) if ("try-error" != class(try(read.csv(csv.file, nrows=5, skip=skip, fill=FALSE), silent=TRUE))) break
    header <- !all(grepl("^X", colnames(read.csv(csv.file, skip=skip, nrows=5))))
    rows <- read.csv(csv.file, header=header, skip=skip, stringsAsFactors=FALSE)
    colnames(rows) <- sub("^V1$", "X", colnames(rows))
    rows$X <- as.character(rows$X)
    rows$periodicity <- sapply(rows$X, function(s) {
      s <- gsub(" ", "", s)
      switch(
        as.character(nchar(s)),
        `0` = "header",
        `8` = "daily",
        `6` = "monthly",
        `4` = "yearly",
        "section"
      )
    })
    sections <- split(rows, cumsum(1:nrow(rows) %in% which(rows$periodicity=="section")))
    names(sections) <- c("Default", rows$X[rows$periodicity=="section"])
    if (!is.null(section)) {
      section.rows <- sections[[section]]
      if (nrow(section.rows) < 3) stop("section contains no data")
      section.rows <- section.rows[3:nrow(section.rows), ] # skip section name and duplicate header
    } else {
      section.rows <- sections[["Default"]]
    }
    if (section.rows$periodicity[1] == "monthly") section.rows$X <- paste0(section.rows$X, "01") # add DD to YYYYMM
    if (section.rows$periodicity[1] == "yearly") section.rows$X <- paste0(section.rows$X, "0630") # add MMDD to YYYY
    section.rows <- section.rows[as.Date(section.rows$X, format="%Y%m%d") >= start, ]
    columns <- if (!is.null(field)) field else setdiff(colnames(section.rows), c("X", "periodicity"))
    xts(data.matrix(section.rows[, columns, drop=FALSE]), as.Date(section.rows$X, format="%Y%m%d"))
  }, finally={
    setwd(wd)
    unlink(extract.dir)
  })
}

ffRetrieveBreakpoints <- function(zip.file, section=NULL, field=NULL, start=as.Date("1980-01-01"), skip=NULL, skip.columns=1) {
  # Retrieves Fama/French timeseries of breakpoints specifically. Differs from
  # `ffRetrieveData` in skipping the first results column by default, and in
  # naming the retrieved columns (as "quantilefive.1", "quantilefive.2", etc.).
  #
  # Args:
  #   zip.file: The source file to process, as provided by Ken French. You can
  #     omit the path and the extension.
  #   section (optional): The section to retrieve from the file. Sources, as
  #     provided by Ken French, can contain different sections, for example the
  #     monthly factors CSV also contains an "Annual Factors: January-December "
  #     section. NOTE: you have to match the section name exactly, and specify
  #     NULL to retrieve the first/only section (which is often un-named).
  #   field (optional): A vector of columns to retrieve. This could be, for
  #     example `c("Banks", "Fin")`.
  #   start (optional): The first date in the timeseries to retrieve. Default is
  #     `as.Date("1980-01-01")`.
  #   skip (optional): The number of lines to skip before processing the file.
  #     NOTE: the function can only skip up to 5 lines automatically, if this
  #     parameter is left to its default value, `NULL`.
  #   skip.columns (optional): Skip the first n number of columns in results.
  #     Those are the number of companies for each row, not of much use for the
  #     timeseries of breakpoints.
  #
  # Returns:
  #   The specified Fama/French timeseries, as an `xts` matrix. NOTE: dimensions
  #   are never dropped in the returned results, the function won't return an
  #   `xts` vector even when a single field is specified or present in the file.
  #
  # Examples:
  # ffRetrieveBreakpoints("ME_Breakpoints_CSV")
  # ffRetrieveBreakpoints("BE-ME_Breakpoints_CSV.zip", skip.columns=2)
  
  breakpoints <- ffRetrieveData(zip.file, section, field, start, skip)
  breakpoints <- breakpoints[, (1 + skip.columns):ncol(breakpoints)] # remove number of companies <=0 and > 0
  colnames(breakpoints) <- paste("quantilefive", 1:ncol(breakpoints), sep=".")
  breakpoints
}
