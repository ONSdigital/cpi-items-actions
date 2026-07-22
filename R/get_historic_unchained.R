library(tidyverse)

# Download from most recent unchained from GitHub 
df_start <- read_csv("new_unchained.csv") 

# https://www.ons.gov.uk/economy/inflationandpriceindices/datasets/consumerpriceindicescpiandretailpricesindexrpiitemindicesandpricequotes
feb_cpi <- read_csv("historic-data/upload-consumptionsegments202602.csv") |> 
  select(INDEX_DATE, CS_ID, CPI_INDEX)
mar_cpi <- read_csv("historic-data/upload-consumptionsegments202603.csv") |> 
  select(INDEX_DATE, CS_ID, CPI_INDEX)
apr_cpi <- read_csv("historic-data/upload-consumptionsegments202604.csv") |> 
  select(INDEX_DATE, CS_ID, CPI_INDEX)
may_cpi <- read_csv("historic-data/upload-consumptionsegments202605.csv") |> 
  select(INDEX_DATE, CS_ID, CPI_INDEX)

all_new_data <- bind_rows(
  feb_cpi, mar_cpi, apr_cpi, may_cpi
) |> 
  mutate(INDEX_DATE = parse_ym(INDEX_DATE)) |> 
  pivot_wider(names_from = INDEX_DATE, values_from = CPI_INDEX) |> 
  filter(!(CS_ID %in% df_start$ITEM_ID))
 
test_start <- df_start |> 
  add_row(
    ITEM_ID = all_new_data$CS_ID,
    `2026-02-01` = all_new_data$`2026-02-01`,
    `2026-03-01` = all_new_data$`2026-03-01`,
    `2026-04-01` = all_new_data$`2026-04-01`,
    `2026-05-01` = all_new_data$`2026-05-01`
  )

new_unchained <- test_start |> 
  filter(ITEM_ID %in% meta$CONSUMPTION_SEGMENT_CODE)
write_csv(new_unchained, "unchained.csv", na = "")  

file.remove("new_unchained.csv")
