library(openxlsx)

# Save timestamp to text file
writeLines(format(Sys.time(), "%Y-%b-%d"), "timestamp.txt")

update <- TRUE

if (update) {
  
  # Set average price reference month
  avgpriceRefMonth <- as.Date("2025-01-01")
  
  # Starting reference point
  startref <- as.Date("2020-01-01")
  
  # -----------------------------
  # Helper functions
  # -----------------------------
  
  split_at <- function(strng, sep, pos) {
    parts <- strsplit(strng, sep, fixed = TRUE)[[1]]
    c(
      paste(parts[seq_len(pos)], collapse = sep),
      paste(parts[(pos + 1):length(parts)], collapse = sep)
    )
  }
  
  parse_yyyymm <- function(x) {
    x <- as.character(x)
    as.Date(paste0(x, "01"), format = "%Y%m%d")
  }
  
  add_months <- function(date, months) {
    lt <- as.POSIXlt(date)
    lt$mon <- lt$mon + months
    as.Date(lt)
  }
  
  add_years <- function(date, years) {
    add_months(date, years * 12)
  }
  
  read_url_text <- function(url) {
    options(HTTPUserAgent = "Mozilla/5.0")
    paste(readLines(url, warn = FALSE), collapse = "\n")
  }
  
  extract_dataset_uris <- function(json_text) {
    m <- gregexpr('"uri"[[:space:]]*:[[:space:]]*"([^"]+)"', json_text, perl = TRUE)
    matches <- regmatches(json_text, m)[[1]]
    sub('.*"uri"[[:space:]]*:[[:space:]]*"([^"]+)".*', "\\1", matches, perl = TRUE)
  }
  
  extract_download_file <- function(json_text) {
    m <- regexpr('"file"[[:space:]]*:[[:space:]]*"([^"]+)"', json_text, perl = TRUE)
    match <- regmatches(json_text, m)
    sub('.*"file"[[:space:]]*:[[:space:]]*"([^"]+)".*', "\\1", match, perl = TRUE)
  }
  
  read_csv_from_url <- function(url) {
    tmp <- tempfile(fileext = ".csv")
    options(HTTPUserAgent = "Mozilla/5.0")
    download.file(url, tmp, quiet = TRUE, mode = "wb")
    read.csv(tmp, check.names = FALSE, stringsAsFactors = FALSE)
  }
  
  to_numeric_df <- function(x) {
    x[] <- lapply(x, function(col) as.numeric(as.character(col)))
    x
  }
  
  date_col <- function(date) {
    format(as.Date(date), "%Y-%m-%d")
  }
  
  replace_sheet <- function(wb, sheet_name, data, row_names = TRUE) {
    if (sheet_name %in% names(wb)) {
      removeWorksheet(wb, sheet_name)
    }
    
    addWorksheet(wb, sheet_name)
    
    writeData(
      wb,
      sheet = sheet_name,
      x = data,
      rowNames = row_names,
      keepNA = FALSE
    )
  }
  
  # -----------------------------
  # Read metadata
  # -----------------------------
  
  meta <- read.csv(
    "./metadata.csv",
    row.names = 1,
    check.names = FALSE,
    stringsAsFactors = FALSE
  )
  
  meta$ITEM_START <- parse_yyyymm(meta$ITEM_START)
  meta$AVERAGE_PRICE <- as.numeric(meta$AVERAGE_PRICE)
  
  # -----------------------------
  # Read unchained CSV
  # -----------------------------
  
  unchained <- read.csv(
    "unchained.csv",
    check.names = FALSE,
    stringsAsFactors = FALSE
  )
  
  latestmonth <- as.Date(names(unchained)[ncol(unchained)], format = "%Y-%m-%d")
  
  # -----------------------------
  # Get data.json from ONS page
  # -----------------------------
  
  data_url <- "https://www.ons.gov.uk/economy/inflationandpriceindices/datasets/consumerpriceindicescpiandretailpricesindexrpiitemindicesandpricequotes/data"
  
  data_json <- read_url_text(data_url)
  datasets <- extract_dataset_uris(data_json)
  
  match <- NA_integer_
  
  for (i in seq_along(datasets)) {
    if (
      !grepl("framework", datasets[i], fixed = TRUE) &&
      !grepl("glossary", datasets[i], fixed = TRUE) &&
      !grepl("/pricequotes", datasets[i], fixed = TRUE)
    ) {
      match <- i
      break
    }
  }
  
  items <- datasets[match]
  message("dataset=", items)
  
  # Get month and year from URI
  date_text <- split_at(items, "consumptionsegmentindices", 1)[2]
  message("the date from url:", date_text)
  
  itemmonth <- as.Date(paste0("01 ", date_text), format = "%d %B%Y")
  
  # -----------------------------
  # Check whether update required
  # -----------------------------
  
  if (itemmonth != latestmonth) {
    
    message("month from indices is different to latest month in unchained csv")
    
    # Download item page JSON
    item_page_url <- paste0("https://www.ons.gov.uk", items, "/data")
    itemspage_json <- read_url_text(item_page_url)
    
    csv_file <- extract_download_file(itemspage_json)
    
    # Download latest indices CSV
    download_url <- paste0("https://www.ons.gov.uk/file?uri=", items, "/", csv_file)
    
    df <- read_csv_from_url(download_url)
    
    names(df)[names(df) == "CS_ID"] <- "ITEM_ID"
    names(df)[names(df) == "CS_DESC"] <- "ITEM_DESC"
    names(df)[names(df) == "CPI_INDEX"] <- "ALL_GM_INDEX"
    names(df)[names(df) == "CPIH_WEIGHT"] <- "ITEM_WEIGHT"
    
    # Get index date from first cell
    index_date <- df[1, 1]
    index_date <- parse_yyyymm(index_date)
    index_date_name <- date_col(index_date)
    
    # Join latest index onto existing CSV
    df_join <- df[, c("ITEM_ID", "ALL_GM_INDEX")]
    names(df_join)[names(df_join) == "ALL_GM_INDEX"] <- index_date_name
    
    unchained$.ord <- seq_len(nrow(unchained))
    
    un <- merge(
      unchained,
      df_join,
      by = "ITEM_ID",
      all.x = TRUE,
      sort = FALSE
    )
    
    un <- un[order(un$.ord), ]
    un$.ord <- NULL
    
    # If last date is Jan, then chain it to December
    last_col <- names(un)[ncol(un)]
    
    if (as.POSIXlt(as.Date(last_col))$mon + 1 == 1) {
      message("chaining jan")
      
      jancol <- names(un)[ncol(un)]
      prevdec <- names(un)[ncol(un) - 1]
      
      un[[jancol]] <- as.numeric(un[[prevdec]]) * as.numeric(un[[jancol]]) / 100
    }
    
    rownames(un) <- un$ITEM_ID
    un$ITEM_ID <- NULL
    
    un <- to_numeric_df(un)
    
    # -----------------------------
    # Create chained indices
    # -----------------------------
    
    chained <- un
    
    for (col in names(chained)) {
      col_date <- as.Date(col)
      
      for (i in rownames(chained)) {
        row_value <- chained[i, col]
        
        item_start <- meta[i, "ITEM_START"]
        
        if (col_date >= item_start) {
          
          if (col_date == startref) {
            
            chained[i, col] <- 100
            
          } else if (col_date <= add_years(startref, 1)) {
            
            chained[i, col] <- row_value
            
          } else {
            
            if (
              as.POSIXlt(col_date)$mon + 1 == 1 &&
              col_date > add_years(startref, 1)
            ) {
              ref_col <- date_col(
                as.Date(
                  paste0(as.POSIXlt(col_date)$year + 1900 - 1, "-01-01")
                )
              )
              
              chained[i, col] <-
                as.numeric(row_value) *
                as.numeric(chained[i, ref_col]) /
                100
              
            } else {
              ref_col <- date_col(
                as.Date(
                  paste0(as.POSIXlt(col_date)$year + 1900, "-01-01")
                )
              )
              
              chained[i, col] <-
                as.numeric(row_value) *
                as.numeric(chained[i, ref_col]) /
                100
            }
          }
          
        } else if (col_date == add_months(item_start, -1)) {
          
          chained[i, col] <- 100
          
        } else {
          
          chained[i, col] <- NA_real_
          
        }
      }
    }
    
    # -----------------------------
    # Calculate average prices
    # -----------------------------
    
    avgprice <- chained
    
    for (col in names(avgprice)) {
      for (i in rownames(avgprice)) {
        row_value <- avgprice[i, col]
        
        if (is.na(row_value)) {
          avgprice[i, col] <- NA_real_
        } else {
          avgprice[i, col] <-
            as.numeric(row_value) /
            as.numeric(chained[i, date_col(avgpriceRefMonth)]) *
            as.numeric(meta[i, "AVERAGE_PRICE"])
        }
      }
    }
    
    write.csv(
      round(avgprice, 2),
      "avgprice.csv",
      na = "",
      row.names = TRUE
    )
    
    # -----------------------------
    # Calculate annual growth
    # -----------------------------
    
    annualgrowth <- chained
    
    for (col in names(annualgrowth)) {
      col_date <- as.Date(col)
      
      for (i in rownames(annualgrowth)) {
        row_value <- annualgrowth[i, col]
        item_start <- meta[i, "ITEM_START"]
        
        if (col_date < add_months(item_start, 11)) {
          
          annualgrowth[i, col] <- NA_real_
          
        } else {
          
          if (col_date < add_years(startref, 1)) {
            
            annualgrowth[i, col] <- NA_real_
            
          } else {
            
            previous_col <- date_col(add_years(col_date, -1))
            
            annualgrowth[i, col] <-
              (as.numeric(row_value) - as.numeric(chained[i, previous_col])) *
              100 /
              as.numeric(chained[i, previous_col])
          }
        }
      }
    }
    
    write.csv(
      round(annualgrowth, 0),
      "annualgrowth.csv",
      na = "",
      row.names = TRUE
    )
    
    # -----------------------------
    # Calculate monthly growth
    # -----------------------------
    
    monthlygrowth <- chained
    
    for (col in names(monthlygrowth)) {
      col_date <- as.Date(col)
      
      for (i in rownames(monthlygrowth)) {
        row_value <- monthlygrowth[i, col]
        item_start <- meta[i, "ITEM_START"]
        
        if (col_date < item_start) {
          
          monthlygrowth[i, col] <- NA_real_
          
        } else {
          
          if (col_date < add_months(startref, 1)) {
            
            monthlygrowth[i, col] <- NA_real_
            
          } else {
            
            previous_col <- date_col(add_months(col_date, -1))
            
            monthlygrowth[i, col] <-
              (as.numeric(row_value) - as.numeric(chained[i, previous_col])) *
              100 /
              as.numeric(chained[i, previous_col])
          }
        }
      }
    }
    
    write.csv(
      round(monthlygrowth, 0),
      "monthlygrowth.csv",
      na = "",
      row.names = TRUE
    )
    
    # -----------------------------
    # Save unchained and chained CSVs
    # -----------------------------
    
    write.csv(
      un,
      "unchained.csv",
      na = "",
      row.names = TRUE
    )
    
    write.csv(
      round(chained, 3),
      "chained.csv",
      na = "",
      row.names = TRUE
    )
    
    # -----------------------------
    # Write Excel datadownload file
    # -----------------------------
    
    if (file.exists("datadownload.xlsx")) {
      wb <- loadWorkbook("datadownload.xlsx")
    } else {
      wb <- createWorkbook()
    }
    
    meta_xlsx <- meta[, names(meta) != "AVERAGE_PRICE", drop = FALSE]
    
    replace_sheet(
      wb = wb,
      sheet_name = "metadata",
      data = meta_xlsx,
      row_names = TRUE
    )
    
    replace_sheet(
      wb = wb,
      sheet_name = "chained",
      data = round(chained, 3),
      row_names = TRUE
    )
    
    replace_sheet(
      wb = wb,
      sheet_name = "averageprice",
      data = round(avgprice, 2),
      row_names = TRUE
    )
    
    replace_sheet(
      wb = wb,
      sheet_name = "monthlygrowth",
      data = round(monthlygrowth, 0),
      row_names = TRUE
    )
    
    replace_sheet(
      wb = wb,
      sheet_name = "annualgrowth",
      data = round(annualgrowth, 0),
      row_names = TRUE
    )
    
    saveWorkbook(
      wb,
      file = "datadownload.xlsx",
      overwrite = TRUE
    )
    
  } else {
    
    message("Nothing to update")
    
  }
  
} else {
  
  message("Automatic updates disabled")
  
}