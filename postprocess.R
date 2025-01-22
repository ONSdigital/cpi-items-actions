library(readr)
library(dplyr)
library(httr)
library(jsonlite)
library(lubridate)
library(openxlsx)

# Save timestamp to text file
writeLines(format(Sys.time(), "%Y-%b-%d"), 'timestamp.txt')

# Set average price reference month
avgpriceRefMonth <- as.Date('2023-01-01')

# Starting reference point
startref <- as.Date('2018-01-01')

# Read in metadata
meta <- read_csv('./metadata.csv') %>%
  mutate(ITEM_START = as.Date(paste0(ITEM_START, "01"), format="%Y%m%d"))

# Define function to split string at a certain occurrence of a separator
split_string <- function(strng, sep, pos) {
  parts <- unlist(strsplit(strng, sep))
  return(paste(parts[1:(pos-1)], collapse=sep), paste(parts[pos:length(parts)], collapse=sep))
}

# Read in unchained CSV
unchained <- read_csv('unchained.csv')

# Find the last month in the unchained file
latestmonth <- as.Date(colnames(unchained)[ncol(unchained)], format="%Y-%m-%d")

# Get the data.json from CPI items and prices page
url <- "https://www.ons.gov.uk/economy/inflationandpriceindices/datasets/consumerpriceindicescpiandretailpricesindexrpiitemindicesandpricequotes/data"
response <- GET(url, add_headers('User-Agent' = 'Mozilla/5.0'))
data <- fromJSON(content(response, "text"))

# Find the first dataset that does not contain the words 'framework', 'glossary', or '/pricequotes'
if (is.data.frame(data$datasets)) {
  match <- NULL
  # Loop through each row of the datasets data frame
  for (i in seq_len(nrow(data$datasets))) {
    # Extract the uri for the current dataset
    uri <- data$datasets$uri[i]
    
    # Check if the 'uri' does not contain 'framework', 'glossary', or '/pricequotes'
    if (!grepl("framework", uri) && 
        !grepl("glossary", uri) && 
        !grepl("/pricequotes", uri)) {
      match <- i
      break
    }
  }
  
  if (!is.null(match)) {
    items <- data$datasets$uri[match]
    cat("Dataset = ", items, "\n")
  } else {
    cat("No matching dataset found.\n")
  }
} else {
  cat("Error: 'datasets' is not a data frame.\n")
}

# Get the month and year from the uri
# Step 1: Split the string by 'itemindices'
date_part <- strsplit(items, "/itemindices")[[1]][2]  # Extract the part after 'itemindices'

# Step 2: Isolate the month-year part (remove any extra characters like slashes)
# We remove any non-alphabetical characters or any forward slashes that might exist
date <- gsub("[^a-zA-Z0-9]", "", date_part)  # Remove anything that isn't a letter or number
cat("The date from URL: ", date, "\n")


# Parse it as a date
itemmonth <- as.Date(paste0(date, "01"), format="%B%Y%d")

# Check date to see if you need to download a file
if (itemmonth != latestmonth) {
  cat("Month from indices is different to latest month in unchained csv\n")
  
  # Download the file
  response <- GET(paste0("https://www.ons.gov.uk", items, "/data"), 
                  add_headers('User-Agent' = 'Mozilla/5.0'))
  itemspage <- fromJSON(content(response, "text"))
  csv <- itemspage$downloads$file[1]  # Extracts the value from the first (and only) row in the 'file' column
  
  # Print the result to verify
  cat("CSV file name: ", csv, "\n")
  
  # Get the CSV of the latest indices
  response <- GET(paste0("https://www.ons.gov.uk/file?uri=", items, "/", csv), 
                  add_headers('User-Agent' = 'Mozilla/5.0'))
  df <- read_csv(content(response, "text"))
  
  # Get the index date which is the first cell
  index_date <- df[1, 1]
  
  # Parse columns as dates in unchained
  # Use `rename_with()` to apply `as.Date()` only to valid date-like column names
  unchained <- unchained %>%
    rename_with(
      ~ ifelse(
        grepl("^\\d{4}-\\d{2}-\\d{2}$", .),  # Check if the column name matches "YYYY-MM-DD"
        as.character(as.Date(., format="%Y-%m-%d")),  # Convert to Date and then to character
        .  # Leave the column name unchanged if it doesn't match the date format
      )
    )
  
  # Join with the new dataset
  un <- left_join(unchained, df %>% select(ITEM_ID, ALL_GM_INDEX) %>% 
                    rename(!!as.name(as.character(index_date)) := ALL_GM_INDEX), 
                  by = "ITEM_ID")
  
  # Check if the last column is in January (column names should be Date objects)
  last_column_name <- names(unchained)[ncol(unchained)]
  last_column_date <- as.Date(last_column_name, format="%Y-%m-%d")
  
  if (month(last_column_date) == 1) {
    cat("Chaining January\n")
    
    # Get the last and previous month columns
    jancol <- names(unchained)[ncol(unchained)]
    prevdec <- names(unchained)[ncol(unchained) - 1]
    
    # Apply chaining logic
    for (index in 1:nrow(unchained)) {
      un[index, jancol] <- un[index, prevdec] * un[index, jancol] / 100
    }
  }
  
  # Create a copy of unchained to create the chained indices
  chained <- un
  
  
  
  # IT GETS UP TO HERE AND THEN BREAKS, SOMETHING TO DO WITH HAVING DATES AS COLUMNS AND COMPARING DATES /SHRUG
  
  
  
  # Create chained indices
  # Loop over columns in chained
  for (col_name in names(chained)) {
    
    # Convert column name to Date (assuming format is "YYYY-MM-DD")
    col <- as.Date(col_name, format="%Y-%m-%d")
    
    # Loop through rows in meta
    for (i in 1:nrow(chained)) {
      
      # Check if col and meta$ITEM_START[i] are valid dates (not NA)
      if (!is.na(col) && !is.na(meta$ITEM_START[i])) {
        
        # If col is greater than or equal to meta$ITEM_START[i], then perform calculations
        if (col >= meta$ITEM_START[i]) {
          
          # Handle specific case when col == startref
          if (col == startref) {
            chained[i, col_name] <- 100
            
          } else if (col <= startref %m+% years(1)) {
            chained[i, col_name] <- chained[i, col_name]  # No change
            
          } else {
            
            # Handle chaining logic: get the previous year's January
            prev_col <- as.Date(paste0(year(col) - 1, "-01-01"))
            
            # Check if prev_col exists in chained (it should be a column name)
            prev_col_name <- as.character(prev_col)  # Convert Date to string (for column reference)
            
            # Only proceed if prev_col_name exists in the dataframe
            if (prev_col_name %in% names(chained)) {
              if (month(col) == 1 && col > startref %m+% years(1)) {
                chained[i, col_name] <- chained[i, col_name] * chained[i, prev_col_name] / 100
              } else {
                chained[i, col_name] <- chained[i, col_name] * chained[i, as.character(as.Date(paste0(year(col), "-01-01")))] / 100
              }
            } else {
              # Handle case where prev_col is not available (set NA or some fallback)
              chained[i, col_name] <- NA
            }
          }
          
        } else if (!is.na(meta$ITEM_START[i]) && col == (meta$ITEM_START[i] %m-% months(1))) {
          chained[i, col_name] <- 100
        } else {
          chained[i, col_name] <- NA
        }
      } else {
        chained[i, col_name] <- NA  # If either col or meta$ITEM_START[i] is NA, set to NA
      }
    }
  }
  
  # Calculate average prices
  avgprice <- chained
  for (col in names(avgprice)) {
    for (i in 1:nrow(avgprice)) {
      if (is.na(avgprice[i, col])) {
        avgprice[i, col] <- NA
      } else {
        avgprice[i, col] <- avgprice[i, col] / chained[i, avgpriceRefMonth] * meta$AVERAGE_PRICE[i]
      }
    }
  }
  
  # Rename columns to dates without time formats
  colnames(avgprice) <- as.Date(names(avgprice), format="%Y-%m-%d")
  
  # Save avgprice to CSV
  write_csv(avgprice, 'avgprice.csv')
  
  # Calculate annual growth
  annualgrowth <- chained
  for (col in names(annualgrowth)) {
    for (i in 1:nrow(annualgrowth)) {
      if (col < meta$ITEM_START[i] %m+% years(1) %m-% months(1)) {
        annualgrowth[i, col] <- NA
      } else if (col < startref %m+% years(1)) {
        annualgrowth[i, col] <- NA
      } else {
        annualgrowth[i, col] <- (annualgrowth[i, col] - chained[i, col %m-% years(1)]) * 100 / chained[i, col %m-% years(1)]
      }
    }
  }
  
  # Rename columns to dates without time formats
  colnames(annualgrowth) <- as.Date(names(annualgrowth), format="%Y-%m-%d")
  
  # Save annualgrowth to CSV
  write_csv(annualgrowth, 'annualgrowth.csv')
  
  # Calculate monthly growth
  monthlygrowth <- chained
  for (col in names(monthlygrowth)) {
    for (i in 1:nrow(monthlygrowth)) {
      if (col < meta$ITEM_START[i]) {
        monthlygrowth[i, col] <- NA
      } else if (col < startref %m+% months(1)) {
        monthlygrowth[i, col] <- NA
      } else {
        monthlygrowth[i, col] <- (monthlygrowth[i, col] - chained[i, col %m-% months(1)]) * 100 / chained[i, col %m-% months(1)]
      }
    }
  }
  
  # Rename columns to dates without time formats
  colnames(monthlygrowth) <- as.Date(names(monthlygrowth), format="%Y-%m-%d")
  
  # Save monthlygrowth to CSV
  write_csv(monthlygrowth, 'monthlygrowth.csv')
  
  # Save unchained and chained numbers to CSV
  write_csv(unchained, 'unchained.csv')
  write_csv(chained, 'chained.csv')
  
  # Create Excel file
  wb <- createWorkbook()
  addWorksheet(wb, "metadata")
  writeData(wb, "metadata", meta %>% select(-AVERAGE_PRICE))
  addWorksheet(wb, "chained")
  writeData(wb, "chained", chained)
  addWorksheet(wb, "averageprice")
  writeData(wb, "averageprice", avgprice)
  addWorksheet(wb, "monthlygrowth")
  writeData(wb, "monthlygrowth", monthlygrowth)
  addWorksheet(wb, "annualgrowth")
  writeData(wb, "annualgrowth", annualgrowth)
  saveWorkbook(wb, "datadownload.xlsx", overwrite = TRUE)
  
} else {
  cat("Nothing to update\n")
}
