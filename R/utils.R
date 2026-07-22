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

# Helper: split string
split_at <- function(strng, sep, pos) {
  parts <- strsplit(strng, sep, fixed = TRUE)[[1]]
  c(
    paste(parts[seq_len(pos)], collapse = sep),
    paste(parts[(pos + 1):length(parts)], collapse = sep)
  )
}

# Helper: Read from URL
read_url_text <- function(url) {
  options(HTTPUserAgent = "Mozilla/5.0")
  paste(readLines(url, warn = FALSE), collapse = "\n")
}

# Helper: check recent date
check_recent_data <- function() {
  data_url <- "https://www.ons.gov.uk/economy/inflationandpriceindices/datasets/consumerpriceindicescpiandretailpricesindexrpiitemindicesandpricequotes/data"
  raw_json_data <- read_url_text(data_url)
  data_json <- fromJSON(raw_json_data)
  datasets <- data_json$datasets$uri
  match <- which(
    !grepl("microdata", datasets, fixed = TRUE) &
      !grepl("framework", datasets, fixed = TRUE) &
      !grepl("glossary", datasets, fixed = TRUE) &
      !grepl("/pricequotes", datasets, fixed = TRUE)
  )[1]
  items <- datasets[match]
  message("dataset=", items)
  # Get month and year from URI
  date_text <- split_at(items, "consumptionsegmentindices", 1)[2]
  message("the date from url:", date_text)
  itemmonth <- as.Date(paste0("01 ", date_text), format = "%d %B%Y")
  return(list(date_text = date_text, itemmonth = itemmonth))
}

# Helper: Extract download file name
extract_download_file <- function(json_text) {
  m <- regexpr('"file"[[:space:]]*:[[:space:]]*"([^"]+)"', json_text, perl = TRUE)
  match <- regmatches(json_text, m)
  sub('.*"file"[[:space:]]*:[[:space:]]*"([^"]+)".*', "\\1", match, perl = TRUE)
}

# Helper: Download CSV file
read_csv_from_url <- function(url) {
  tmp <- tempfile(fileext = ".csv")
  options(HTTPUserAgent = "Mozilla/5.0")
  download.file(url, tmp, quiet = TRUE, mode = "wb")
  read.csv(tmp, check.names = FALSE, stringsAsFactors = FALSE)
}