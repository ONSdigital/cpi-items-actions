# Process data

library(openxlsx)
library(jsonlite)

source("R/utils.R")


# Set up ------------------------------------------------------------------

## Set mode (manual or auto) ----
manual <- FALSE
 # filenames if manual
test_metafile <- "metadata.csv" # new metadata file from BA
test_new_csv <- "test_newdata_march.csv" # new CPI data
test_unchained <- "unchained_new.csv" # old unchained file


## Reference months ----
avgprice_ref_month <- as.Date("2026-01-01")
startref <- as.Date("2021-01-01")

## Save timestamp ----
writeLines(format(Sys.time(), "%Y-%b-%d"), "timestamp.txt")



# Get recent data ---------------------------------------------------------

if (!manual) {
  # Read in old unchained
  unchained <- read.csv(
    "unchained.csv",
    check.names = FALSE,
    stringsAsFactors = FALSE
  )
  # Compare to newest data
  latestmonth <- as.Date(names(unchained)[ncol(unchained)], format = "%Y-%m-%d")
  recent_data <- check_recent_data()
  itemmonth <- recent_data$itemmonth
  if (itemmonth != latestmonth) {
    new_data_url <- paste0(
      "https://www.ons.gov.uk",
      "/economy/inflationandpriceindices/datasets/consumerpriceindicescpiandretailpricesindexrpiitemindicesandpricequotes/consumptionsegmentindices",
      recent_data$date_text,
      "/data")
    
    itemspage_json <- read_url_text(new_data_url)
    csv_file <- extract_download_file(itemspage_json)
    new_csv <- paste0("https://www.ons.gov.uk/file?uri=",
                      "/economy/inflationandpriceindices/datasets/consumerpriceindicescpiandretailpricesindexrpiitemindicesandpricequotes/consumptionsegmentindices",
                      recent_data$date_text, "/", csv_file)
    # need to read it in
    
    
  } else {
    message("Nothing to update")
  }
} else {
  new_csv <- test_new_csv
}


# Read in metadata --------------------------------------------------------

if (!manual) {
  metafile <- "metadata.csv"
} else {
  metafile <- test_metafile
}

df_meta <- read.csv(
  metafile,
  check.names = FALSE,
  stringsAsFactors = FALSE
)
df_meta$ID_START <- parse_ym(sprintf("%06d", df_meta$ID_START)) 

# Drop ID_NAME, rearrange so CONSUMPTION_SEGMENT_CODE is first, dedupe
# df_meta_deduped <- df_meta[, names(df_meta) != "ID_NAME", drop = FALSE]

df_meta_deduped <- df_meta
other_cols <- names(df_meta_deduped)[names(df_meta_deduped) != "CONSUMPTION_SEGMENT_CODE"]
df_meta_deduped <- df_meta_deduped[, c("CONSUMPTION_SEGMENT_CODE", other_cols), drop = FALSE]



# Two ID name called Taxi fare
df_meta_items <- df_meta_deduped[!duplicated(df_meta_deduped$ID_NAME, df_meta_deduped$CONSUMPTION_SEGMENT_CODE), ]

meta_seg_starts <- df_meta_deduped[,c("CONSUMPTION_SEGMENT_CODE", "ID_START")]
meta_seg_starts <- meta_seg_starts[!duplicated(meta_seg_starts$CONSUMPTION_SEGMENT_CODE), ]

meta <- meta_seg_starts
rownames(meta) <- as.character(meta$CONSUMPTION_SEGMENT_CODE)





# Read old unchained file -------------------------------------------------

if (!manual) {
  unchained_file <- "unchained.csv"
} else {
  unchained_file <- test_unchained
}

df_start <- read.csv(unchained_file,
  stringsAsFactors = FALSE, check.names = FALSE
)

# delete extra columns as testing for Feb
# if (manual) {
#   df_start <- df_start |>
#   dplyr::select(-starts_with("2020")) |>
#     dplyr::select(-matches("^2026-(03|04|05|06)")) |>
#     dplyr::filter(ITEM_ID %in% meta$CONSUMPTION_SEGMENT_CODE)
# }

names(df_start)[names(df_start) == "ITEM_ID"] <- "CONSUMPTION_SEGMENT_CODE"

date_cols <- {
  x <- names(df_start)[grepl("^20\\d{2}-\\d{2}-\\d{2}$", names(df_start))]
  x[as.Date(x) >= startref]
}
unchained_raw <- df_start[, c("CONSUMPTION_SEGMENT_CODE", date_cols), drop = FALSE]
unchained_raw <- unchained_raw[!duplicated(unchained_raw$CONSUMPTION_SEGMENT_CODE), ]
unchained_raw[] <- lapply(unchained_raw, function(col) gsub("-", "", as.character(col)))
write.csv(unchained_raw, "unchained_raw.csv", row.names = FALSE)

cat("Created: unchained_raw.csv\n")


# Read unchained CSV ----
unchained_raw <- read.csv("unchained_raw.csv",
  stringsAsFactors = FALSE,
  check.names = FALSE, colClasses = "character"
)
date_col_names <- names(unchained_raw)[names(unchained_raw) != "CONSUMPTION_SEGMENT_CODE"]
latest_month <- max(date_col_names)
cat("Latest month in unchained:", format(latest_month), "\n")



# Read new data file ------------------------------------------------------

df_new <- read.csv(new_csv,
                    stringsAsFactors = FALSE,
                    check.names = FALSE)


index_date_str <- sprintf("%d", as.integer(df_new[1, 1]))
index_date <- parse_ym(index_date_str)
cat("New data date:", format(index_date), "\n")



# Rename unchained columns from YYYYMM to YYYY-MM-DD
new_col_names <- names(unchained_raw)
for (i in seq_along(new_col_names)) {
  nm <- new_col_names[i]
  if (grepl("^[0-9]{6}$", nm)) new_col_names[i] <- fmt_date(parse_ym(nm))
}
names(unchained_raw) <- new_col_names

# Merge test CPI_INDEX as a new column
test_col_name <- fmt_date(index_date)
test_data <- df_new[, c("CS_ID", "CPI_INDEX")]
names(test_data) <- c("CONSUMPTION_SEGMENT_CODE", test_col_name)
test_data$CONSUMPTION_SEGMENT_CODE <- as.character(test_data$CONSUMPTION_SEGMENT_CODE)
unchained_raw$CONSUMPTION_SEGMENT_CODE <- as.character(unchained_raw$CONSUMPTION_SEGMENT_CODE)

un <- merge(unchained_raw, test_data, by = "CONSUMPTION_SEGMENT_CODE", all.x = TRUE)

rownames(un) <- un$CONSUMPTION_SEGMENT_CODE
un$CONSUMPTION_SEGMENT_CODE <- NULL
un[] <- lapply(un, as.numeric)

# Sort columns chronologically
col_dates <- as.Date(names(un))
un <- un[, order(col_dates), drop = FALSE]

# If last column is January, chain it to prior December
col_dates_sorted <- as.Date(names(un))
last_date <- max(col_dates_sorted)
if (as.integer(format(last_date, "%m")) == 1) {
  cat("chaining jan\n")
  jan_col <- fmt_date(last_date)
  prev_dec <- fmt_date(max(col_dates_sorted[col_dates_sorted < last_date]))
  un[[jan_col]] <- un[[prev_dec]] * un[[jan_col]] / 100
}

# Chain the indices ----
chained <- un

# Columns must be processed in chronological order because later Jan values
# depend on the previously computed Jan chained value.
for (col_name in names(chained)) {
  col_date <- as.Date(col_name)

  for (seg in rownames(chained)) {
    id_start <- meta[seg, "ID_START"]
    row_value <- chained[seg, col_name]

    if (col_date >= id_start) {
      if (col_date == startref) {
        chained[seg, col_name] <- 100
      } else if (col_date <= seq(startref, by = "year", length.out = 2)[2]) {
        chained[seg, col_name] <- row_value
      } else {
        col_month <- as.integer(format(col_date, "%m"))
        col_year <- as.integer(format(col_date, "%Y"))

        if (col_month == 1) {
          prev_jan <- fmt_date(as.Date(paste0(col_year - 1, "-01-01")))
          chained[seg, col_name] <- as.numeric(row_value) *
            as.numeric(chained[seg, prev_jan]) / 100
        } else {
          curr_jan <- fmt_date(as.Date(paste0(col_year, "-01-01")))
          chained[seg, col_name] <- as.numeric(row_value) *
            as.numeric(chained[seg, curr_jan]) / 100
        }
      }
    } else if (col_date == seq(id_start, by = "-1 month", length.out = 2)[2]) {
      chained[seg, col_name] <- 100
    } else {
      chained[seg, col_name] <- NA
    }
  }
}

cat("Chaining complete\n")

# Save unchained and chained to CSV ----
un_out <- cbind(CONSUMPTION_SEGMENT_CODE = rownames(un), un)
write.csv(un_out, "unchained.csv", row.names = FALSE, na = "")

chained_out <- cbind(CONSUMPTION_SEGMENT_CODE = rownames(round(chained, 3)), round(chained, 3))
write.csv(chained_out, "chained.csv", row.names = FALSE, na = "")

cat("Saved: unchained.csv, chained.csv\n")

# Average prices ----
allitems <- read.csv("metadata.csv",
  stringsAsFactors = FALSE,
  check.names = FALSE
)
allitems$CONSUMPTION_SEGMENT_CODE <- as.character(allitems$CONSUMPTION_SEGMENT_CODE)
base_col <- as.character(avgprice_ref_month)
avgprice_merged <- allitems[, c("ID_NAME", "CONSUMPTION_SEGMENT_CODE")]
allitems$AVERAGE_PRICE <- as.numeric(allitems$AVERAGE_PRICE)

for (col_name in names(chained)) {
  avgprice_merged[[col_name]] <- sapply(
    seq_len(nrow(avgprice_merged)),
    function(i) {
      
      seg <- allitems$CONSUMPTION_SEGMENT_CODE[i]
      avg_price <- allitems$AVERAGE_PRICE[i]
      
      if (!(seg %in% rownames(chained))) {
        return(NA_real_)
      }
      
      if (is.na(avg_price)) {
        return(NA_real_)
      }
      
      base_val <- chained[seg, base_col]
      curr_val <- chained[seg, col_name]
      
      if (is.na(base_val) || is.na(curr_val) || base_val == 0) {
        return(NA_real_)
      }
      
      else {
        return(round(curr_val / base_val * avg_price[1], 2))
      }
    }
  )
}

# for (col_name in names(chained)) {
#   avgprice_merged[[col_name]] <- sapply(avgprice_merged$CONSUMPTION_SEGMENT_CODE, function(seg) {
#     if (!(seg %in% rownames(chained))) {
#       return(NA)
#     }
#     avg_price_row <- allitems[allitems$CONSUMPTION_SEGMENT_CODE == seg, "AVERAGE_PRICE"]
#     if (length(avg_price_row) == 0 || is.na(avg_price_row[1])) {
#       return(NA)
#     }
#     base_val <- chained[seg, base_col]
#     curr_val <- chained[seg, col_name]
#     if (is.na(base_val) || is.na(curr_val) || base_val == 0) {
#       return(NA)
#     } else {
#       return(round(curr_val / base_val * avg_price_row[1], 2))
#     }
#   })
# }

write.csv(avgprice_merged, "avgprice.csv", row.names = FALSE, na = "")
cat("Saved: avgprice.csv\n")

# Monthly growth ----
allitems <- read.csv("metadata.csv",
  stringsAsFactors = FALSE,
  check.names = FALSE
)
allitems$CONSUMPTION_SEGMENT_CODE <- as.character(allitems$CONSUMPTION_SEGMENT_CODE)

monthly_growth <- allitems[, c("ID_NAME", "CONSUMPTION_SEGMENT_CODE")]
col_names <- names(chained)

for (idx in seq(2, length(col_names))) {
  col_name <- col_names[idx]
  prev_col <- col_names[idx - 1]
  monthly_growth[[col_name]] <- sapply(monthly_growth$CONSUMPTION_SEGMENT_CODE, function(seg) {
    if (!(seg %in% rownames(chained))) {
      return(NA)
    }
    prev <- chained[seg, prev_col]
    curr <- chained[seg, col_name]
    if (is.na(prev) || is.na(curr) || prev == 0) {
      return(NA)
    }
    round((curr - prev) * 100 / prev, 3)
  })
}

write.csv(monthly_growth, "monthly_growth.csv", row.names = FALSE, na = "")
cat("Saved: monthly_growth.csv\n")

# Annual growth ----
allitems <- read.csv("metadata.csv",
  stringsAsFactors = FALSE,
  check.names = FALSE
)
allitems$CONSUMPTION_SEGMENT_CODE <- as.character(allitems$CONSUMPTION_SEGMENT_CODE)

annual_growth <- allitems[, c("ID_NAME", "CONSUMPTION_SEGMENT_CODE")]
col_names <- names(chained)

for (idx in seq(13, length(col_names))) {
  col_name <- col_names[idx]
  prev_col <- col_names[idx - 12]
  annual_growth[[col_name]] <- sapply(annual_growth$CONSUMPTION_SEGMENT_CODE, function(seg) {
    if (!(seg %in% rownames(chained))) {
      return(NA)
    }
    prev <- chained[seg, prev_col]
    curr <- chained[seg, col_name]
    if (is.na(prev) || is.na(curr) || prev == 0) {
      return(NA)
    }
    round((curr - prev) * 100 / prev, 3)
    # as.integer(round((curr - prev) * 100 / prev))
  })
}

write.csv(annual_growth, "annual_growth.csv", row.names = FALSE, na = "")
cat("Saved: annual_growth.csv\n")

# Excel datadownload ----
meta_for_datadownload <- read.csv("metadata.csv", 
  stringsAsFactors = FALSE,
  check.names = FALSE
)

meta_for_datadownload$CONSUMPTION_SEGMENT_CODE <- as.character(
  meta_for_datadownload$CONSUMPTION_SEGMENT_CODE
)
meta_sheet <- meta_for_datadownload[, names(meta_for_datadownload) != "AVERAGE_PRICE",
  drop = FALSE
]
meta_sheet <- meta_sheet[, c(
  "ID_START", "ID_NAME", "WEIGHT_SIZE", "CONSUMPTION_SEGMENT_CODE", "CONSUMPTION_SEGMENT_NAME", "Category1", 
  "Category2", "COICOP5_NAME", "COICOP4_NAME", "COICOP3_NAME", "COICOP2_NAME"
)]
names(meta_sheet)[names(meta_sheet) == "ID_NAME"] <- "ID_DESC"
names(meta_sheet)[names(meta_sheet) == "CONSUMPTION_SEGMENT_NAME"] <- "CONSUMPTION_SEGMENT_DESC"
names(meta_sheet)[names(meta_sheet) == "COICOP5_NAME"] <- "COICOP5_DESC"
names(meta_sheet)[names(meta_sheet) == "COICOP4_NAME"] <- "COICOP4_DESC"
names(meta_sheet)[names(meta_sheet) == "COICOP3_NAME"] <- "COICOP3_DESC"
names(meta_sheet)[names(meta_sheet) == "COICOP2_NAME"] <- "COICOP2_DESC"
meta_sheet <- meta_sheet[
  order(
    meta_sheet$Category1,
    meta_sheet$Category2,
    meta_sheet$CONSUMPTION_SEGMENT_CODE,
    meta_sheet$ID_DESC
  ), ]

## Chained ----

chained_long <- melt_wide(
  cbind(CONSUMPTION_SEGMENT_CODE = rownames(chained), round(chained, 3)),
  id_cols = "CONSUMPTION_SEGMENT_CODE"
)
chained_long <- chained_long[!is.na(suppressWarnings(as.numeric(chained_long$Value))), ]
names(chained_long)[names(chained_long) == "Value"] <- "Value"
chained_long <- merge(
  chained_long,
  meta_for_datadownload[, c(
    "ID_NAME", "CONSUMPTION_SEGMENT_CODE",
    "Category1", "Category2", "WEIGHT_SIZE"
  )]
)
chained_long <- chained_long[, c(
  "Date", "ID_NAME", "WEIGHT_SIZE", "CONSUMPTION_SEGMENT_CODE", "Category1", 
  "Category2", "Value"
)]
names(chained_long)[names(chained_long) == "Date"] <- "INDEX_DATE"
names(chained_long)[names(chained_long) == "Value"] <- "INDEX_VALUE"
names(chained_long)[names(chained_long) == "ID_NAME"] <- "ID_DESC"
chained_long <- chained_long[
  order(
    chained_long$Category1,
    chained_long$Category2,
    chained_long$CONSUMPTION_SEGMENT_CODE,
    chained_long$ID_DESC,
    -xtfrm(chained_long$INDEX_DATE)
  ), ]

chained_long <- chained_long[
  ave(
    seq_len(nrow(chained_long)),
    chained_long$CONSUMPTION_SEGMENT_CODE,
    FUN = length
  ) > 1,
] 
 


## Average price ----

avgprice_long <- melt_wide(avgprice_merged, id_cols = c("ID_NAME", "CONSUMPTION_SEGMENT_CODE"))
avgprice_long$Value <- as.numeric(avgprice_long$Value)
names(avgprice_long)[names(avgprice_long) == "Value"] <- "AVERAGE_PRICE"
avgprice_long <- merge(
  avgprice_long,
  meta_for_datadownload[, c(
    "ID_NAME", "CONSUMPTION_SEGMENT_CODE",
    "Category1", "Category2", "WEIGHT_SIZE"
  )]
)
avgprice_long <- avgprice_long[, c(
  "Date", "ID_NAME", "WEIGHT_SIZE", "CONSUMPTION_SEGMENT_CODE", "Category1", 
  "Category2", "AVERAGE_PRICE"
)]

names(avgprice_long)[names(avgprice_long) == "Date"] <- "INDEX_DATE"
names(avgprice_long)[names(avgprice_long) == "ID_NAME"] <- "ID_DESC"
avgprice_long <- avgprice_long[
  order(
    avgprice_long$Category1,
    avgprice_long$Category2,
    avgprice_long$CONSUMPTION_SEGMENT_CODE,
    avgprice_long$ID_DESC,
    -xtfrm(avgprice_long$INDEX_DATE)
    ), ]

## Monthly ----

monthly_long <- melt_wide(monthly_growth, id_cols = c("ID_NAME", "CONSUMPTION_SEGMENT_CODE"))
monthly_long$Value <- as.numeric(monthly_long$Value)
monthly_long <- monthly_long[!is.na(monthly_long$Value), ]
names(monthly_long)[names(monthly_long) == "Value"] <- "Percentage"
monthly_long <- merge(
  monthly_long,
  meta_for_datadownload[, c(
    "ID_NAME", "CONSUMPTION_SEGMENT_CODE",
    "Category1", "Category2", "WEIGHT_SIZE"
  )]
)
monthly_long <- monthly_long[, c(
  "Date", "ID_NAME", "WEIGHT_SIZE", "CONSUMPTION_SEGMENT_CODE", "Category1", 
  "Category2", "Percentage"
)]
names(monthly_long)[names(monthly_long) == "Percentage"] <- "PERCENTAGE_GROWTH"
names(monthly_long)[names(monthly_long) == "Date"] <- "INDEX_DATE"
names(monthly_long)[names(monthly_long) == "ID_NAME"] <- "ID_DESC"

monthly_long <- monthly_long[
  order(
    monthly_long$Category1,
    monthly_long$Category2,
    monthly_long$CONSUMPTION_SEGMENT_CODE,
    monthly_long$ID_DESC,
    -xtfrm(monthly_long$INDEX_DATE)
  ), ]

## Anuual ----

annual_long <- melt_wide(annual_growth, id_cols = c("ID_NAME", "CONSUMPTION_SEGMENT_CODE"))
annual_long$Value <- as.numeric(annual_long$Value)
annual_long <- annual_long[!is.na(annual_long$Value), ]
names(annual_long)[names(annual_long) == "Value"] <- "Percentage"
annual_long <- merge(
  annual_long,
  meta_for_datadownload[, c(
    "ID_NAME", "CONSUMPTION_SEGMENT_CODE",
    "Category1", "Category2", "WEIGHT_SIZE"
  )]
)
annual_long <- annual_long[, c(
  "Date", "ID_NAME", "WEIGHT_SIZE", "CONSUMPTION_SEGMENT_CODE", "Category1", 
  "Category2", "Percentage"
)]
names(annual_long)[names(annual_long) == "Percentage"] <- "PERCENTAGE_GROWTH"
names(annual_long)[names(annual_long) == "Date"] <- "INDEX_DATE"
names(annual_long)[names(annual_long) == "ID_NAME"] <- "ID_DESC"

annual_long <- annual_long[
  order(
    annual_long$Category1,
    annual_long$Category2,
    annual_long$CONSUMPTION_SEGMENT_CODE,
    annual_long$ID_DESC,
    -xtfrm(annual_long$INDEX_DATE)
  ), ]



wb <- loadWorkbook("datadownload.xlsx")
for (sheet_name in c("Metadata", "Average price", "Chained index", "Monthly growth", "Annual growth")) {
  if (sheet_name %in% names(wb)) removeWorksheet(wb, sheet_name)
  addWorksheet(wb, sheet_name)
}
writeData(wb, "Metadata", meta_sheet)
writeData(wb, "Average price", avgprice_long, keepNA = TRUE, na.string = "-")
writeData(wb, "Chained index", chained_long)
writeData(wb, "Monthly growth", monthly_long)
writeData(wb, "Annual growth", annual_long)

# Format as 2 dp for average price
addStyle(
  wb,
  "Average price",
  style = createStyle(numFmt = "0.00"),
  rows = 2:(nrow(avgprice_long) + 1),
  cols = c(7), 
  gridExpand = TRUE
)
# 3 dp for perc growth
addStyle(
  wb,
  "Chained index",
  style = createStyle(numFmt = "0.000"),
  rows = 2:(nrow(chained_long) + 1),
  cols = c(7), 
  gridExpand = TRUE
)
addStyle(
  wb,
  "Monthly growth",
  style = createStyle(numFmt = "0.000"),
  rows = 2:(nrow(monthly_long) + 1),
  cols = c(7), 
  gridExpand = TRUE
)
addStyle(
  wb,
  "Annual growth",
  style = createStyle(numFmt = "0.000"),
  rows = 2:(nrow(annual_long) + 1),
  cols = c(7), 
  gridExpand = TRUE
)


saveWorkbook(wb, "datadownload.xlsx", overwrite = TRUE)
cat("Saved: datadownload.xlsx\n")


# filter out unavailable items from avg prices merged and resave
# filter out rows where all values are NA
avgprice_merged <- avgprice_merged[
  rowSums(
    !is.na(
      avgprice_merged[
        , !names(avgprice_merged) %in% c("ID_NAME", "CONSUMPTION_SEGMENT_CODE")
      ]
    )
  ) > 0,
]

write.csv(avgprice_merged, "avgprice.csv", row.names = FALSE, na = "")
cat("Saved: avgprice.csv\n")

# Remove extra unchained file
file.remove("unchained_raw.csv")

# Finished
cat("R process complete.\n")

