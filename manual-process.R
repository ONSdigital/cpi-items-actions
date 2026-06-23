# manual-process.R
# R script equivalent of manual-process.ipynb
# Uses local test data (march_2026_example_test_data_v1.csv)
# rather than live ONS download.

library(openxlsx)

# Functions ---------------------------------------------------------------

# Helper: parse a YYYYMM string as a Date (first of month)
parse_ym <- function(x) {
  as.Date(paste0(x, "01"), format = "%Y%m%d")
}

# Helper: format a Date as YYYY-MM-DD string
fmt_date <- function(d) {
  format(as.Date(d), "%Y-%m-%d")
}

# Helper: wide to long format
melt_wide <- function(df, id_cols) {
  id_data <- df[, id_cols, drop = FALSE]
  val_cols <- names(df)[!names(df) %in% id_cols]
  do.call(rbind, lapply(val_cols, function(cn) {
    row <- id_data
    row$Date <- cn
    row$Value <- df[[cn]]
    row
  }))
}


# Save timestamp ----
writeLines(format(Sys.time(), "%Y-%b-%d"), "timestamp.txt")

# Create metadata and unchained files from starting file ----
df_meta <- read.csv("2026_starting_file_test_data_v2.csv",
  skip = 2,
  stringsAsFactors = FALSE, check.names = FALSE
)

# Drop columns after AVERAGE_PRICE
if ("AVERAGE_PRICE" %in% names(df_meta)) {
  last_col_idx <- which(names(df_meta) == "AVERAGE_PRICE")
  df_meta <- df_meta[, 1:last_col_idx, drop = FALSE]
}

# Save all-items metadata (used later for avgprice/growth with ID_NAME)
write.csv(df_meta, "2026_all_items_metadata.csv", row.names = FALSE)

# Drop ID_NAME, rearrange so CONSUMPTION_SEGMENT_CODE is first, dedupe
df_meta_deduped <- df_meta[, names(df_meta) != "ID_NAME", drop = FALSE]
other_cols <- names(df_meta_deduped)[names(df_meta_deduped) != "CONSUMPTION_SEGMENT_CODE"]
df_meta_deduped <- df_meta_deduped[, c("CONSUMPTION_SEGMENT_CODE", other_cols), drop = FALSE]
df_meta_deduped <- df_meta_deduped[!duplicated(df_meta_deduped$CONSUMPTION_SEGMENT_CODE), ]
write.csv(df_meta_deduped, "2025_metadata.csv", row.names = FALSE)

# Create unchained file: keep CONSUMPTION_SEGMENT_CODE + date columns >= 202101
df_start <- read.csv("2026_starting_file_test_data_v2.csv",
  skip = 2,
  stringsAsFactors = FALSE, check.names = FALSE
)
date_cols <- names(df_start)[grepl("^20[0-9]{4}$", names(df_start)) & names(df_start) >= "202101"]
unchained2025 <- df_start[, c("CONSUMPTION_SEGMENT_CODE", date_cols), drop = FALSE]
unchained2025 <- unchained2025[!duplicated(unchained2025$CONSUMPTION_SEGMENT_CODE), ]
unchained2025[] <- lapply(unchained2025, function(col) gsub("-", "", as.character(col)))
write.csv(unchained2025, "2025_unchained.csv", row.names = FALSE)

cat("Created: 2026_all_items_metadata.csv, 2025_metadata.csv, 2025_unchained.csv\n")

# Reference months ----
avgprice_ref_month <- as.Date("2026-01-01")
startref <- as.Date("2022-01-01")

# Read metadata ----
meta <- read.csv("2025_metadata.csv", stringsAsFactors = FALSE)
meta$ID_START <- parse_ym(sprintf("%06d", meta$ID_START))
rownames(meta) <- as.character(meta$CONSUMPTION_SEGMENT_CODE)

# Read unchained CSV ----
unchained_raw <- read.csv("2025_unchained.csv",
  stringsAsFactors = FALSE,
  check.names = FALSE, colClasses = "character"
)
date_col_names <- names(unchained_raw)[names(unchained_raw) != "CONSUMPTION_SEGMENT_CODE"]
latest_month <- parse_ym(max(date_col_names))
cat("Latest month in unchained:", format(latest_month), "\n")

# Merge test data (manual/local workflow) ----
df_test <- read.csv("march_2026_example_test_data_v1.csv",
  stringsAsFactors = FALSE,
  check.names = FALSE
)
index_date_str <- sprintf("%d", as.integer(df_test[1, 1]))
index_date <- parse_ym(index_date_str)
cat("Test data date:", format(index_date), "\n")

# Rename unchained columns from YYYYMM to YYYY-MM-DD
new_col_names <- names(unchained_raw)
for (i in seq_along(new_col_names)) {
  nm <- new_col_names[i]
  if (grepl("^[0-9]{6}$", nm)) new_col_names[i] <- fmt_date(parse_ym(nm))
}
names(unchained_raw) <- new_col_names

# Merge test CPI_INDEX as a new column
test_col_name <- fmt_date(index_date)
test_data <- df_test[, c("CS_ID", "CPI_INDEX")]
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
allitems <- read.csv("2026_all_items_metadata.csv",
  stringsAsFactors = FALSE,
  check.names = FALSE
)
allitems$CONSUMPTION_SEGMENT_CODE <- as.character(allitems$CONSUMPTION_SEGMENT_CODE)

## why is it checking the base value when calculating the average price.
## if the base value i.e. the avg price in Jan 2022 is NA, then avg price NA
# base_col <- names(chained)[1]
base_col <- as.character(avgprice_ref_month)
avgprice_merged <- allitems[, c("ID_NAME", "CONSUMPTION_SEGMENT_CODE")]

for (col_name in names(chained)) {
  avgprice_merged[[col_name]] <- sapply(avgprice_merged$CONSUMPTION_SEGMENT_CODE, function(seg) {
    if (!(seg %in% rownames(chained))) {
      return(NA)
    }
    avg_price_row <- allitems[allitems$CONSUMPTION_SEGMENT_CODE == seg, "AVERAGE_PRICE"]
    if (length(avg_price_row) == 0 || is.na(avg_price_row[1])) {
      return(NA)
    }
    base_val <- chained[seg, base_col]
    curr_val <- chained[seg, col_name]
    if (is.na(base_val) || is.na(curr_val) || base_val == 0) {
      return(NA)
    }
    round(curr_val / base_val * avg_price_row[1], 2)
  })
}

write.csv(avgprice_merged, "avgprice_merged.csv", row.names = FALSE, na = "")
cat("Saved: avgprice_merged.csv\n")

# Monthly growth ----
allitems <- read.csv("2026_all_items_metadata.csv",
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
    as.integer(round((curr - prev) * 100 / prev))
  })
}

write.csv(monthly_growth, "monthly_growth_merged.csv", row.names = FALSE, na = "")
cat("Saved: monthly_growth_merged.csv\n")

# Annual growth ----
allitems <- read.csv("2026_all_items_metadata.csv",
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
    round((curr - prev) * 100 / prev, 1)
    # as.integer(round((curr - prev) * 100 / prev))
  })
}

write.csv(annual_growth, "annual_growth_merged.csv", row.names = FALSE, na = "")
cat("Saved: annual_growth_merged.csv\n")

# Excel datadownload ----
meta_for_datadownload <- read.csv("2026_all_items_metadata.csv",
  stringsAsFactors = FALSE,
  check.names = FALSE
)
meta_for_datadownload$CONSUMPTION_SEGMENT_CODE <- as.character(
  meta_for_datadownload$CONSUMPTION_SEGMENT_CODE
)


meta_sheet <- meta_for_datadownload[, names(meta_for_datadownload) != "AVERAGE_PRICE",
  drop = FALSE
]

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
    "Category1", "Category2", "WEIGHT\\SIZE"
  )]
)
chained_long <- chained_long[, c(
  "ID_NAME", "Category1", "CONSUMPTION_SEGMENT_CODE",
  "Category2", "WEIGHT\\SIZE", "Date", "Value"
)]

avgprice_long <- melt_wide(avgprice_merged, id_cols = c("ID_NAME", "CONSUMPTION_SEGMENT_CODE"))
avgprice_long$Value <- as.numeric(avgprice_long$Value)
avgprice_long <- avgprice_long[!is.na(avgprice_long$Value), ]
names(avgprice_long)[names(avgprice_long) == "Value"] <- "Price"
avgprice_long <- merge(
  avgprice_long,
  meta_for_datadownload[, c(
    "ID_NAME", "CONSUMPTION_SEGMENT_CODE",
    "Category1", "Category2", "WEIGHT\\SIZE"
  )]
)
avgprice_long <- avgprice_long[, c(
  "CONSUMPTION_SEGMENT_CODE", "ID_NAME",
  "Category1", "Category2", "WEIGHT\\SIZE", "Date", "Price"
)]

monthly_long <- melt_wide(monthly_growth, id_cols = c("ID_NAME", "CONSUMPTION_SEGMENT_CODE"))
monthly_long$Value <- as.numeric(monthly_long$Value)
monthly_long <- monthly_long[!is.na(monthly_long$Value), ]
names(monthly_long)[names(monthly_long) == "Value"] <- "Percentage"
monthly_long <- merge(
  monthly_long,
  meta_for_datadownload[, c(
    "ID_NAME", "CONSUMPTION_SEGMENT_CODE",
    "Category1", "Category2", "WEIGHT\\SIZE"
  )]
)
monthly_long <- monthly_long[, c(
  "CONSUMPTION_SEGMENT_CODE", "ID_NAME",
  "Category1", "Category2", "WEIGHT\\SIZE", "Date", "Percentage"
)]

annual_long <- melt_wide(annual_growth, id_cols = c("ID_NAME", "CONSUMPTION_SEGMENT_CODE"))
annual_long$Value <- as.numeric(annual_long$Value)
annual_long <- annual_long[!is.na(annual_long$Value), ]
names(annual_long)[names(annual_long) == "Value"] <- "Percentage"
annual_long <- merge(
  annual_long,
  meta_for_datadownload[, c(
    "ID_NAME", "CONSUMPTION_SEGMENT_CODE",
    "Category1", "Category2", "WEIGHT\\SIZE"
  )]
)
annual_long <- annual_long[, c(
  "CONSUMPTION_SEGMENT_CODE", "ID_NAME",
  "Category1", "Category2", "WEIGHT\\SIZE", "Date", "Percentage"
)]

wb <- loadWorkbook("datadownload.xlsx")
for (sheet_name in c("Metadata", "Chained", "Average price", "Monthly growth", "Annual growth")) {
  if (sheet_name %in% names(wb)) removeWorksheet(wb, sheet_name)
  addWorksheet(wb, sheet_name)
}
writeData(wb, "Metadata", meta_sheet)
writeData(wb, "Chained", chained_long)
writeData(wb, "Average price", avgprice_long)
writeData(wb, "Monthly growth", monthly_long)
writeData(wb, "Annual growth", annual_long)
saveWorkbook(wb, "datadownload.xlsx", overwrite = TRUE)

cat("Saved: datadownload.xlsx\n")
cat("R process complete.\n")
