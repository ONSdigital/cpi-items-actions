
library(tidyverse)
old_unchained <- file.choose()

new_starting <- read_csv("2026_starting_file_test_data_v2.csv", skip = 2)


old_data <- read_csv(old_unchained)


filter_old_data <- old_data |> 
  select(-starts_with("2020")) |> 
  select(-matches("^2026-(03|04|05|06)"))


new_data <- new_starting |> 
  select(-starts_with("20")) |> 
  left_join(filter_old_data, by = c("CONSUMPTION_SEGMENT_CODE" = "ITEM_ID")) |> 
  mutate(
    across(
      everything(), ~if_else(is.na(.x), "-", as.character(.x))
    )
  ) 
colnames(new_data) <- colnames(new_data) |> 
str_remove("-01$") |> 
  str_remove("-")

write_csv(new_data, "starting_file.csv")
