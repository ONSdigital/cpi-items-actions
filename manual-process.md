---
title: "CPI Items Manual Process"
output:
  html_notebook:
    toc: true
    toc_float: true
---

# Setup


``` r
library(openxlsx)

# Helper: parse a YYYYMM string as a Date (first of month)
parse_ym <- function(x) as.Date(paste0(x, "01"), format = "%Y%m%d")

# Helper: format a Date as YYYY-MM-DD string
fmt_date <- function(d) format(as.Date(d), "%Y-%m-%d")
```

# Save timestamp


``` r
writeLines(format(Sys.time(), "%Y-%b-%d"), "timestamp.txt")
```

# Create metadata and unchained files from starting file


``` r
# Read starting file (skip 2 header rows)
df_meta <- read.csv("2026_starting_file_test_data_v2.csv", skip = 2,
                    stringsAsFactors = FALSE, check.names = FALSE)

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
df_start <- read.csv("2026_starting_file_test_data_v2.csv", skip = 2,
                     stringsAsFactors = FALSE, check.names = FALSE)
date_pattern <- "^20[0-9]{4}$"
date_cols <- names(df_start)[grepl(date_pattern, names(df_start)) & names(df_start) >= "202101"]
unchained2025 <- df_start[, c("CONSUMPTION_SEGMENT_CODE", date_cols), drop = FALSE]
unchained2025 <- unchained2025[!duplicated(unchained2025$CONSUMPTION_SEGMENT_CODE), ]
# Replace hyphens with empty string
unchained2025[] <- lapply(unchained2025, function(col) gsub("-", "", as.character(col)))
write.csv(unchained2025, "2025_unchained.csv", row.names = FALSE)

cat("Created: 2026_all_items_metadata.csv, 2025_metadata.csv, 2025_unchained.csv\n")
```

```
## Created: 2026_all_items_metadata.csv, 2025_metadata.csv, 2025_unchained.csv
```

# Set reference months


``` r
avgprice_ref_month <- as.Date("2026-01-01")
startref          <- as.Date("2022-01-01")
```

# Read metadata


``` r
meta <- read.csv("2025_metadata.csv", stringsAsFactors = FALSE)
# ID_START is stored as YYYYMM integer - parse to Date
meta$ID_START <- parse_ym(sprintf("%06d", meta$ID_START))
rownames(meta) <- as.character(meta$CONSUMPTION_SEGMENT_CODE)
meta
```

```
##           CONSUMPTION_SEGMENT_CODE   ID_START
## 220209                      220209 2003-02-01
## 220210                      220210 2003-02-01
## 220323                      220323 2003-02-01
## 220326                      220326 2012-02-01
## 220327                      220327 2018-02-01
## 410518                      410518 2005-01-01
## 430307                      430307 1987-02-01
## 430317                      430317 1999-02-01
## 430321                      430321 1999-02-01
## 430419                      430419 2001-02-01
## 430423                      430423 2005-01-01
## 430622                      430622 2003-02-01
## 430623                      430623 2005-01-01
## 440101                      440101 1987-02-01
## 510132                      510132 2013-02-01
## 510133                      510133 2022-02-01
## 510134                      510134 2023-02-01
## 510402                      510402 1987-02-01
## 510415                      510415 1987-02-01
## 510435                      510435 2023-02-01
## 510533                      510533 2011-02-01
## 510537                      510537 2026-02-01
## 520209                      520209 1987-02-01
## 520213                      520213 1987-02-01
## 520215                      520215 1987-02-01
## 520216                      520216 1987-02-01
## 520223                      520223 1993-02-01
## 520301                      520301 1987-02-01
## 520303                      520303 1987-02-01
## 610115                      610115 2024-02-01
## 610204                      610204 1987-02-01
## 610207                      610207 1987-02-01
## 610309                      610309 2007-02-01
## 610310                      610310 2007-02-01
## 630168                      630168 2025-02-01
## 630377                      630377 2018-02-01
## 640211                      640211 1987-02-01
## 640212                      640212 1987-02-01
## CP0111101                CP0111101 2026-02-01
## CP0111301                CP0111301 2026-02-01
## CP0112301                CP0112301 2026-02-01
## CP0113301                CP0113301 2026-02-01
## CP0113302                CP0113302 2026-02-01
## CP0115101                CP0115101 2026-02-01
## CP0116501                CP0116501 2026-02-01
## CP0117101                CP0117101 2026-02-01
## CP0117901                CP0117901 2026-02-01
## CP0117906                CP0117906 2026-02-01
## CP0118901                CP0118901 2026-02-01
## CP0118902                CP0118902 2026-02-01
## CP0119202                CP0119202 2026-02-01
## CP0122001                CP0122001 2026-02-01
## CP0211001                CP0211001 2026-02-01
##                                                             CONSUMPTION_SEGMENT_DESC
## 220209                                           Primary school meal (fixed charges)
## 220210                                                  Secondary school (cafeteria)
## 220323                                                                Takeaway kebab
## 220326                                                      Takeaway chicken & chips
## 220327                                                        Takeaway cooked pastry
## 410518                                                                     Carpenter
## 430307                                                               Electric kettle
## 430317                                                                 Electric iron
## 430321                                                                Electric razor
## 430419                                                   Stainless steel cutlery set
## 430423                                                        Frying pan (non-stick)
## 430622                                                               Dog kennel fees
## 430623                                                              Small pet mammal
## 440101                                                              Domestic cleaner
## 510132                                                   Men's short sleeved t-shirt
## 510133                                                      Men's loungewear bottoms
## 510134                                                    Men's formal jacket/blazer
## 510402                                                            Men's pants/boxers
## 510415                                                                Women's tights
## 510435                                                           Sports bra/crop top
## 510533                                                     Women's high heeled shoes
## 510537                                                          Women's canvas shoes
## 520209                                                                    Toothpaste
## 520213                                                                       Tissues
## 520215                                                            Facial moisturiser
## 520216                                                     Deodorant/anti perspirant
## 520223                                                                       Mascara
## 520301                                                                 Men's haircut
## 520303                                         Women's hairdressing (cut & blow dry)
## 610115                                                           Adult electric bike
## 610204                                                                      Car tyre
## 610207                                                               Car spare parts
## 610309                                                      Ultra low sulphur diesel
## 610310                                                      Ultra low sulphur petrol
## 630168                                                                    VR headset
## 630377                                                                 Action camera
## 640211                                                   Historic monument admission
## 640212                                                                Theatre ticket
## CP0111101                                      Rice, in all forms (excl. rice flour)
## CP0111301                                                               Bread, white
## CP0112301                             Cooked ham and continental meats (e.g. salami)
## CP0113301                               Breaded and battered fish, chilled or frozen
## CP0113302                                                                Canned fish
## CP0115101                                                                  Olive oil
## CP0116501                                                 Other fruits n.e.c., fresh
## CP0117101               Cauliflowers, broccoli and other brassicas, fresh or chilled
## CP0117901 Vegetables, tubers and pulses, canned or jarred (excl. pickled vegetables)
## CP0117906                                      Vegetarian and vegan meat substitutes
## CP0118901                                              Sweets, mints and chewing gum
## CP0118902                        Sweets, soft and hard (excl. mints and chewing gum)
## CP0119202                                                               Baby formula
## CP0122001                                                                     Coffee
## CP0211001                                                               Spirits, gin
##                               Category1
## 220209    Food and drink establishments
## 220210    Food and drink establishments
## 220323    Food and drink establishments
## 220326    Food and drink establishments
## 220327    Food and drink establishments
## 410518                         Services
## 430307                  Household items
## 430317                  Household items
## 430321                  Household items
## 430419                  Household items
## 430423                  Household items
## 430622           Recreation and culture
## 430623           Recreation and culture
## 440101                         Services
## 510132            Clothing and footwear
## 510133            Clothing and footwear
## 510134            Clothing and footwear
## 510402            Clothing and footwear
## 510415            Clothing and footwear
## 510435            Clothing and footwear
## 510533            Clothing and footwear
## 510537            Clothing and footwear
## 520209                           Health
## 520213                           Health
## 520215                           Health
## 520216                           Health
## 520223                           Health
## 520301                         Services
## 520303                         Services
## 610115                        Transport
## 610204                        Transport
## 610207                        Transport
## 610309                        Transport
## 610310                        Transport
## 630168           Recreation and culture
## 630377           Recreation and culture
## 640211           Recreation and culture
## 640212           Recreation and culture
## CP0111101                Food and drink
## CP0111301                Food and drink
## CP0112301                Food and drink
## CP0113301                Food and drink
## CP0113302                Food and drink
## CP0115101                Food and drink
## CP0116501                Food and drink
## CP0117101                Food and drink
## CP0117901                Food and drink
## CP0117906                Food and drink
## CP0118901                Food and drink
## CP0118902                Food and drink
## CP0119202                Food and drink
## CP0122001                Food and drink
## CP0211001                Food and drink
##                                                                                             Category2
## 220209                                                                                        Schools
## 220210                                                                                        Schools
## 220323                                                                       Fast food and take aways
## 220326                                                                       Fast food and take aways
## 220327                                                                       Fast food and take aways
## 410518                                                                         Maintenance and repair
## 430307                                                                                     Electrical
## 430317                                                                                     Electrical
## 430321                                                                                     Electrical
## 430419                                                                                       Cookware
## 430423                                                                                       Cookware
## 430622                                                                                           Pets
## 430623                                                                                           Pets
## 440101                                                                         Maintenance and repair
## 510132                                                                                 Men's clothing
## 510133                                                                                 Men's clothing
## 510134                                                                                 Men's clothing
## 510402                                                                                 Men's clothing
## 510415                                                                               Women's clothing
## 510435                                                                               Women's clothing
## 510533                                                                               Women's footwear
## 510537                                                                               Women's footwear
## 520209                                                                  Personal hygiene and wellness
## 520213                                                                  Personal hygiene and wellness
## 520215                                                                  Personal hygiene and wellness
## 520216                                                                  Personal hygiene and wellness
## 520223                                                                  Personal hygiene and wellness
## 520301                                                                                   Hairdressing
## 520303                                                                                   Hairdressing
## 610115                                                                    Bikes and cycling equipment
## 610204                                                                                    Spare parts
## 610207                                                                                    Spare parts
## 610309                                                                              Petrol and diesel
## 610310                                                                              Petrol and diesel
## 630168                                                                     Audio and visual equipment
## 630377                                                                     Audio and visual equipment
## 640211                                                                                       Activity
## 640212                                                                                       Activity
## CP0111101                                                                              Store cupboard
## CP0111301                                                                                      Bakery
## CP0112301                                                                                        Meat
## CP0113301                                                                                        Fish
## CP0113302                                                                                        Fish
## CP0115101                                                                    Butter, spreads and oils
## CP0116501                                                                                 Fresh fruit
## CP0117101                                                                            Fresh vegetables
## CP0117901                                                                              Store cupboard
## CP0117906                                                                                  Other food
## CP0118901                                                                        Chocolate and sweets
## CP0118902                                                                        Chocolate and sweets
## CP0119202                                                                                  Other food
## CP0122001 Soft drinks                                                                                
## CP0211001                                                                                     Alcohol
##                                                               COICOP5
## 220209                                                       Canteens
## 220210                                                       Canteens
## 220323                          Fast food and take away food services
## 220326                          Fast food and take away food services
## 220327                          Fast food and take away food services
## 410518                                         Services of carpenters
## 430307             Coffee machines; tea makers and similar appliances
## 430317                                                          Irons
## 430321    Electric appliances for personal care - Not being published
## 430419                               Cutlery; flatware and silverware
## 430423                     Non-electric kitchen utensils and articles
## 430622                         Veterinary and other services for pets
## 430623                                               Purchase of pets
## 440101                 Other domestic services and household services
## 510132                                               Garments for men
## 510133                                               Garments for men
## 510134                                               Garments for men
## 510402                                               Garments for men
## 510415                                             Garments for women
## 510435                                             Garments for women
## 510533                                             Footwear for women
## 510537                                             Footwear for women
## 520209                     Articles for personal hygiene and wellness
## 520213                     Articles for personal hygiene and wellness
## 520215                     Articles for personal hygiene and wellness
## 520216                     Articles for personal hygiene and wellness
## 520223                     Articles for personal hygiene and wellness
## 520301                              Hairdressing for men and children
## 520303                                         Hairdressing for women
## 610115                                                   Motor cycles
## 610204                                                          Tyres
## 610207                   Spare parts for personal transport equipment
## 610309                                                         Diesel
## 610310                                                         Petrol
## 630168               Accessories for information processing equipment
## 630377            Photographic, cinematographic and optical equipment
## 640211                         Museums; libraries; zoological gardens
## 640212                                    Cinemas; theatres; concerts
## CP0111101                            Food and non-alcoholic beverages
## CP0111301                            Food and non-alcoholic beverages
## CP0112301                            Food and non-alcoholic beverages
## CP0113301                            Food and non-alcoholic beverages
## CP0113302                            Food and non-alcoholic beverages
## CP0115101                            Food and non-alcoholic beverages
## CP0116501                            Food and non-alcoholic beverages
## CP0117101                            Food and non-alcoholic beverages
## CP0117901                            Food and non-alcoholic beverages
## CP0117906                            Food and non-alcoholic beverages
## CP0118901                            Food and non-alcoholic beverages
## CP0118902                            Food and non-alcoholic beverages
## CP0119202                            Food and non-alcoholic beverages
## CP0122001                            Food and non-alcoholic beverages
## CP0211001                             Alcoholic beverages and tobacco
##                                                       COICOP4
## 220209                                               Canteens
## 220210                                               Canteens
## 220323                                     Restaurants, cafes
## 220326                                     Restaurants, cafes
## 220327                                     Restaurants, cafes
## 410518                    Services for maintenance and repair
## 430307            / Major appliances and small electric goods
## 430317            / Major appliances and small electric goods
## 430321            / Appliances and products for personal care
## 430419            Glassware, Tableware and Household Utensils
## 430423            Glassware, Tableware and Household Utensils
## 430622                  / Pets, related products and services
## 430623                  / Pets, related products and services
## 440101               Domestic services and household services
## 510132                                               Garments
## 510133                                               Garments
## 510134                                               Garments
## 510402                                               Garments
## 510415                                               Garments
## 510435                                               Garments
## 510533                             Footwear including repairs
## 510537                             Footwear including repairs
## 520209            / Appliances and products for personal care
## 520213            / Appliances and products for personal care
## 520215            / Appliances and products for personal care
## 520216            / Appliances and products for personal care
## 520223            / Appliances and products for personal care
## 520301      Hairdressing and personal grooming establishments
## 520303      Hairdressing and personal grooming establishments
## 610115                             / Motorcycles and bicycles
## 610204                            Spare parts and accessories
## 610207                            Spare parts and accessories
## 610309                                   Fuels and lubricants
## 610310                                   Fuels and lubricants
## 630168                              Data processing equipment
## 630377    Photographic, cinematographic and optical equipment
## 640211                                      Cultural services
## 640212                                      Cultural services
## CP0111101                                                Food
## CP0111301                                                Food
## CP0112301                                                Food
## CP0113301                                                Food
## CP0113302                                                Food
## CP0115101                                                Food
## CP0116501                                                Food
## CP0117101                                                Food
## CP0117901                                                Food
## CP0117906                                                Food
## CP0118901                                                Food
## CP0118902                                                Food
## CP0119202                                                Food
## CP0122001                             Non-alcoholic beverages
## CP0211001                                 Alcoholic beverages
##                                                   COICOP3
## 220209                                  Catering services
## 220210                                  Catering services
## 220323                                  Catering services
## 220326                                  Catering services
## 220327                                  Catering services
## 410518     Regular maintenance and repair of the dwelling
## 430307          Household appliances, fitting and repairs
## 430317          Household appliances, fitting and repairs
## 430321                                      Personal care
## 430419        Glassware, tableware and household utensils
## 430423        Glassware, tableware and household utensils
## 430622         Other recreational items, gardens and pets
## 430623         Other recreational items, gardens and pets
## 440101         Goods and services for routine maintenance
## 510132                                           Clothing
## 510133                                           Clothing
## 510134                                           Clothing
## 510402                                           Clothing
## 510415                                           Clothing
## 510435                                           Clothing
## 510533                         Footwear including repairs
## 510537                         Footwear including repairs
## 520209                                      Personal care
## 520213                                      Personal care
## 520215                                      Personal care
## 520216                                      Personal care
## 520223                                      Personal care
## 520301                                      Personal care
## 520303                                      Personal care
## 610115                               Purchase of vehicles
## 610204          Operation of personal transport equipment
## 610207          Operation of personal transport equipment
## 610309          Operation of personal transport equipment
## 610310          Operation of personal transport equipment
## 630168        Audio-visual equipment and related products
## 630377        Audio-visual equipment and related products
## 640211                 Recreational and cultural services
## 640212                 Recreational and cultural services
## CP0111101                               Bread and cereals
## CP0111301                               Bread and cereals
## CP0112301                                            Meat
## CP0113301                                            Fish
## CP0113302                                            Fish
## CP0115101                                   Oils and fats
## CP0116501                                           Fruit
## CP0117101        Vegetables including potatoes and tubers
## CP0117901        Vegetables including potatoes and tubers
## CP0117906        Vegetables including potatoes and tubers
## CP0118901 Sugar, jam, syrups, chocolate and confectionery
## CP0118902 Sugar, jam, syrups, chocolate and confectionery
## CP0119202                           Milk, cheese and eggs
## CP0122001                           Coffee, tea and cocoa
## CP0211001                                         Spirits
##                                                             COICOP2
## 220209                                       Restaurants and hotels
## 220210                                       Restaurants and hotels
## 220323                                       Restaurants and hotels
## 220326                                       Restaurants and hotels
## 220327                                       Restaurants and hotels
## 410518             Housing, water, electricity, gas and other fuels
## 430307               Furniture, household equipment and maintenance
## 430317               Furniture, household equipment and maintenance
## 430321                             Miscellaneous goods and services
## 430419               Furniture, household equipment and maintenance
## 430423               Furniture, household equipment and maintenance
## 430622                                       Recreation and culture
## 430623                                       Recreation and culture
## 440101               Furniture, household equipment and maintenance
## 510132                                        Clothing and footwear
## 510133                                        Clothing and footwear
## 510134                                        Clothing and footwear
## 510402                                        Clothing and footwear
## 510415                                        Clothing and footwear
## 510435                                        Clothing and footwear
## 510533                                        Clothing and footwear
## 510537                                        Clothing and footwear
## 520209                             Miscellaneous goods and services
## 520213                             Miscellaneous goods and services
## 520215                             Miscellaneous goods and services
## 520216                             Miscellaneous goods and services
## 520223                             Miscellaneous goods and services
## 520301                             Miscellaneous goods and services
## 520303                             Miscellaneous goods and services
## 610115                                                    Transport
## 610204                                                    Transport
## 610207                                                    Transport
## 610309                                                    Transport
## 610310                                                    Transport
## 630168                                       Recreation and culture
## 630377                                       Recreation and culture
## 640211                                       Recreation and culture
## 640212                                       Recreation and culture
## CP0111101                                                      Rice
## CP0111301                                                     Bread
## CP0112301                              Dried; salted or smoked meat
## CP0113301        Other pres or processed fish & seafood-based preps
## CP0113302        Other pres or processed fish & seafood-based preps
## CP0115101                                                 Olive oil
## CP0116501                                    Fresh or chilled fruit
## CP0117101        Fresh/chill veg other than potatoes & other tubers
## CP0117901 Dried vegetables; other preserved or processed vegetables
## CP0117906 Dried vegetables; other preserved or processed vegetables
## CP0118901                                    Confectionery products
## CP0118902                                    Confectionery products
## CP0119202                                       Other milk products
## CP0122001                                                    Coffee
## CP0211001                                                   Spirits
##            WEIGHT.SIZE AVERAGE_PRICE
## 220209                          2.37
## 220210                          2.97
## 220323                          7.06
## 220326                          7.23
## 220327                          1.99
## 410518     hourly rate         26.31
## 430307    1.5-1.7litre         30.79
## 430317                         42.92
## 430321                            NA
## 430419                         17.52
## 430423         24-30cm         18.94
## 430622    daily charge         24.12
## 430623                            NA
## 440101     hourly rate         17.88
## 510132                         17.64
## 510133                         27.80
## 510134                        143.82
## 510402                            NA
## 510415                            NA
## 510435                         28.43
## 510533                            NA
## 510537                         17.71
## 520209                            NA
## 520213       large box          1.63
## 520215        50-150ml            NA
## 520216        50-250ml          2.55
## 520223                         10.04
## 520301                         15.79
## 520303                         46.04
## 610115                       1640.44
## 610204                         81.76
## 610207                            NA
## 610309                          1.36
## 610310                          1.38
## 630168          single        637.14
## 630377                            NA
## 640211                            NA
## 640212       one adult         30.84
## CP0111101        750 g          2.50
## CP0111301        800 g          1.40
## CP0112301        150 g          2.00
## CP0113301      10 pack          3.50
## CP0113302         each          1.50
## CP0115101       200 ml          3.00
## CP0116501       1000 g          3.00
## CP0117101       1000 g          3.00
## CP0117901        412 g          0.50
## CP0117906        360 g          2.50
## CP0118901         each          0.10
## CP0118902        195 g          1.50
## CP0119202        800 g         12.00
## CP0122001         95 g          3.50
## CP0211001       700 ml         20.00
```

# Read unchained CSV


``` r
unchained_raw <- read.csv("2025_unchained.csv", stringsAsFactors = FALSE,
                          check.names = FALSE, colClasses = "character")

# Find latest month
date_col_names <- names(unchained_raw)[names(unchained_raw) != "CONSUMPTION_SEGMENT_CODE"]
latest_month <- parse_ym(max(date_col_names))
cat("Latest month in unchained:", format(latest_month), "\n")
```

```
## Latest month in unchained: 2026-02-01
```

# Merge test data (manual/local workflow)


``` r
# Read the test file
df_test <- read.csv("march_2026_example_test_data_v1.csv", stringsAsFactors = FALSE,
                    check.names = FALSE)
index_date_str <- sprintf("%d", as.integer(df_test[1, 1]))
index_date     <- parse_ym(index_date_str)
cat("Test data date:", format(index_date), "\n")
```

```
## Test data date: 2026-03-01
```

``` r
# Rename unchained columns from YYYYMM to Date objects (as strings YYYY-MM-DD)
new_col_names <- names(unchained_raw)
for (i in seq_along(new_col_names)) {
  nm <- new_col_names[i]
  if (grepl("^[0-9]{6}$", nm)) {
    new_col_names[i] <- fmt_date(parse_ym(nm))
  }
}
names(unchained_raw) <- new_col_names

# Merge test CPI_INDEX column
test_col_name <- fmt_date(index_date)
test_data <- df_test[, c("CS_ID", "CPI_INDEX")]
names(test_data) <- c("CONSUMPTION_SEGMENT_CODE", test_col_name)
test_data$CONSUMPTION_SEGMENT_CODE <- as.character(test_data$CONSUMPTION_SEGMENT_CODE)
unchained_raw$CONSUMPTION_SEGMENT_CODE <- as.character(unchained_raw$CONSUMPTION_SEGMENT_CODE)

un <- merge(unchained_raw, test_data, by = "CONSUMPTION_SEGMENT_CODE", all.x = TRUE)

# Set CONSUMPTION_SEGMENT_CODE as rownames, convert all values to numeric
rownames(un) <- un$CONSUMPTION_SEGMENT_CODE
un$CONSUMPTION_SEGMENT_CODE <- NULL
un[] <- lapply(un, as.numeric)

# Re-order columns chronologically
col_dates <- as.Date(names(un))
un <- un[, order(col_dates), drop = FALSE]

# If last column is January, chain it to December
col_dates_sorted <- as.Date(names(un))
last_date <- max(col_dates_sorted)
if (as.integer(format(last_date, "%m")) == 1) {
  cat("chaining jan\n")
  jan_col  <- fmt_date(last_date)
  prev_dec <- fmt_date(max(col_dates_sorted[col_dates_sorted < last_date]))
  un[[jan_col]] <- un[[prev_dec]] * un[[jan_col]] / 100
}

cat("un dimensions:", nrow(un), "x", ncol(un), "\n")
```

```
## un dimensions: 53 x 51
```

# Chain the indices


``` r
chained <- un

# Process columns in chronological order (important: later months depend on Jan values)
for (col_name in names(chained)) {
  col_date <- as.Date(col_name)

  for (seg in rownames(chained)) {
    id_start  <- meta[seg, "ID_START"]
    row_value <- chained[seg, col_name]

    if (col_date >= id_start) {
      if (col_date == startref) {
        chained[seg, col_name] <- 100

      } else if (col_date <= seq(startref, by = "year", length.out = 2)[2]) {
        # Between startref and startref + 1 year: keep unchained value
        chained[seg, col_name] <- row_value

      } else {
        col_month <- as.integer(format(col_date, "%m"))
        col_year  <- as.integer(format(col_date, "%Y"))

        if (col_month == 1) {
          # January after the first year: chain to previous January
          prev_jan <- fmt_date(as.Date(paste0(col_year - 1, "-01-01")))
          chained[seg, col_name] <- as.numeric(row_value) *
            as.numeric(chained[seg, prev_jan]) / 100
        } else {
          # Other months: chain to current year January
          curr_jan <- fmt_date(as.Date(paste0(col_year, "-01-01")))
          chained[seg, col_name] <- as.numeric(row_value) *
            as.numeric(chained[seg, curr_jan]) / 100
        }
      }

    } else if (col_date == seq(id_start, by = "-1 month", length.out = 2)[2]) {
      # Month before item starts: set to 100
      chained[seg, col_name] <- 100

    } else {
      chained[seg, col_name] <- NA
    }
  }
}

cat("Chaining complete\n")
```

```
## Chaining complete
```

# Save unchained and chained to CSV


``` r
# Save unchained
un_out <- cbind(CONSUMPTION_SEGMENT_CODE = rownames(un), un)
write.csv(un_out, "unchained.csv", row.names = FALSE, na = "")

# Save chained (rounded to 3dp)
chained_rounded <- round(chained, 3)
chained_out <- cbind(CONSUMPTION_SEGMENT_CODE = rownames(chained_rounded), chained_rounded)
write.csv(chained_out, "chained.csv", row.names = FALSE, na = "")

cat("Saved: unchained.csv, chained.csv\n")
```

```
## Saved: unchained.csv, chained.csv
```

# Calculate average prices


``` r
allitems <- read.csv("2026_all_items_metadata.csv", stringsAsFactors = FALSE,
                     check.names = FALSE)
allitems$CONSUMPTION_SEGMENT_CODE <- as.character(allitems$CONSUMPTION_SEGMENT_CODE)

# Base column is the first chained column
base_col <- names(chained)[1]

avgprice_merged <- allitems[, c("ID_NAME", "CONSUMPTION_SEGMENT_CODE")]

for (col_name in names(chained)) {
  avgprice_merged[[col_name]] <- sapply(avgprice_merged$CONSUMPTION_SEGMENT_CODE, function(seg) {
    if (!(seg %in% rownames(chained))) return(NA)
    avg_price_row <- allitems[allitems$CONSUMPTION_SEGMENT_CODE == seg, "AVERAGE_PRICE"]
    if (length(avg_price_row) == 0 || is.na(avg_price_row[1])) return(NA)
    base_val <- chained[seg, base_col]
    curr_val <- chained[seg, col_name]
    if (is.na(base_val) || is.na(curr_val) || base_val == 0) return(NA)
    round(curr_val / base_val * avg_price_row[1], 2)
  })
}

write.csv(avgprice_merged, "avgprice_merged.csv", row.names = FALSE, na = "")
cat("Saved: avgprice_merged.csv\n")
```

```
## Saved: avgprice_merged.csv
```

# Calculate monthly growth


``` r
allitems <- read.csv("2026_all_items_metadata.csv", stringsAsFactors = FALSE,
                     check.names = FALSE)
allitems$CONSUMPTION_SEGMENT_CODE <- as.character(allitems$CONSUMPTION_SEGMENT_CODE)

monthly_growth <- allitems[, c("ID_NAME", "CONSUMPTION_SEGMENT_CODE")]
col_names <- names(chained)

for (idx in seq(2, length(col_names))) {
  col_name  <- col_names[idx]
  prev_col  <- col_names[idx - 1]

  monthly_growth[[col_name]] <- sapply(monthly_growth$CONSUMPTION_SEGMENT_CODE, function(seg) {
    if (!(seg %in% rownames(chained))) return(NA)
    prev <- chained[seg, prev_col]
    curr <- chained[seg, col_name]
    if (is.na(prev) || is.na(curr) || prev == 0) return(NA)
    as.integer(round((curr - prev) * 100 / prev))
  })
}

write.csv(monthly_growth, "monthly_growth_merged.csv", row.names = FALSE, na = "")
cat("Saved: monthly_growth_merged.csv\n")
```

```
## Saved: monthly_growth_merged.csv
```

# Calculate annual growth


``` r
allitems <- read.csv("2026_all_items_metadata.csv", stringsAsFactors = FALSE,
                     check.names = FALSE)
allitems$CONSUMPTION_SEGMENT_CODE <- as.character(allitems$CONSUMPTION_SEGMENT_CODE)

annual_growth <- allitems[, c("ID_NAME", "CONSUMPTION_SEGMENT_CODE")]
col_names <- names(chained)

for (idx in seq(13, length(col_names))) {
  col_name  <- col_names[idx]
  prev_col  <- col_names[idx - 12]

  annual_growth[[col_name]] <- sapply(annual_growth$CONSUMPTION_SEGMENT_CODE, function(seg) {
    if (!(seg %in% rownames(chained))) return(NA)
    prev <- chained[seg, prev_col]
    curr <- chained[seg, col_name]
    if (is.na(prev) || is.na(curr) || prev == 0) return(NA)
    as.integer(round((curr - prev) * 100 / prev))
  })
}

write.csv(annual_growth, "annual_growth_merged.csv", row.names = FALSE, na = "")
cat("Saved: annual_growth_merged.csv\n")
```

```
## Saved: annual_growth_merged.csv
```

# Create Excel datadownload


``` r
meta_for_datadownload <- read.csv("2026_all_items_metadata.csv", stringsAsFactors = FALSE,
                                  check.names = FALSE)
meta_for_datadownload$CONSUMPTION_SEGMENT_CODE <- as.character(
  meta_for_datadownload$CONSUMPTION_SEGMENT_CODE)

# Helper: melt a wide data frame to long format
melt_wide <- function(df, id_cols) {
  id_data  <- df[, id_cols, drop = FALSE]
  val_cols <- names(df)[!names(df) %in% id_cols]
  result   <- do.call(rbind, lapply(val_cols, function(cn) {
    row <- id_data
    row[["Date"]]  <- cn
    row[["Value"]] <- df[[cn]]
    row
  }))
  result
}

# Metadata sheet (no AVERAGE_PRICE)
meta_sheet <- meta_for_datadownload[, names(meta_for_datadownload) != "AVERAGE_PRICE",
                                    drop = FALSE]

# Chained sheet (tidy long format, drop NAs)
chained_long <- melt_wide(
  cbind(CONSUMPTION_SEGMENT_CODE = rownames(chained), round(chained, 3)),
  id_cols = "CONSUMPTION_SEGMENT_CODE"
)
chained_long <- chained_long[!is.na(as.numeric(chained_long$Value)), ]
names(chained_long)[names(chained_long) == "Value"] <- "Value"
chained_long <- merge(chained_long,
                      meta_for_datadownload[, c("ID_NAME", "CONSUMPTION_SEGMENT_CODE",
                                                "Category1", "Category2", "WEIGHT\\SIZE")])
chained_long <- chained_long[, c("ID_NAME", "Category1", "CONSUMPTION_SEGMENT_CODE",
                                  "Category2", "WEIGHT\\SIZE", "Date", "Value")]

# Average price sheet
avgprice_long <- melt_wide(avgprice_merged, id_cols = c("ID_NAME", "CONSUMPTION_SEGMENT_CODE"))
avgprice_long$Value <- as.numeric(avgprice_long$Value)
avgprice_long <- avgprice_long[!is.na(avgprice_long$Value), ]
names(avgprice_long)[names(avgprice_long) == "Value"] <- "Price"
avgprice_long <- merge(avgprice_long,
                       meta_for_datadownload[, c("ID_NAME", "CONSUMPTION_SEGMENT_CODE",
                                                  "Category1", "Category2", "WEIGHT\\SIZE")])
avgprice_long <- avgprice_long[, c("CONSUMPTION_SEGMENT_CODE", "ID_NAME",
                                    "CONSUMPTION_SEGMENT_CODE", "Category1",
                                    "Category2", "WEIGHT\\SIZE", "Price")]
# Fix column de-dup issue from merge
avgprice_long <- avgprice_long[, c("CONSUMPTION_SEGMENT_CODE", "ID_NAME",
                                    "Category1", "Category2", "WEIGHT\\SIZE", "Date", "Price")]
```

```
## Error in `[.data.frame`:
## ! undefined columns selected
```

``` r
# Monthly growth sheet
monthly_long <- melt_wide(monthly_growth, id_cols = c("ID_NAME", "CONSUMPTION_SEGMENT_CODE"))
monthly_long$Value <- as.numeric(monthly_long$Value)
monthly_long <- monthly_long[!is.na(monthly_long$Value), ]
names(monthly_long)[names(monthly_long) == "Value"] <- "Percentage"
monthly_long <- merge(monthly_long,
                      meta_for_datadownload[, c("ID_NAME", "CONSUMPTION_SEGMENT_CODE",
                                                  "Category1", "Category2", "WEIGHT\\SIZE")])
monthly_long <- monthly_long[, c("CONSUMPTION_SEGMENT_CODE", "ID_NAME",
                                   "Category1", "Category2", "WEIGHT\\SIZE", "Date", "Percentage")]

# Annual growth sheet
annual_long <- melt_wide(annual_growth, id_cols = c("ID_NAME", "CONSUMPTION_SEGMENT_CODE"))
annual_long$Value <- as.numeric(annual_long$Value)
annual_long <- annual_long[!is.na(annual_long$Value), ]
names(annual_long)[names(annual_long) == "Value"] <- "Percentage"
annual_long <- merge(annual_long,
                     meta_for_datadownload[, c("ID_NAME", "CONSUMPTION_SEGMENT_CODE",
                                                 "Category1", "Category2", "WEIGHT\\SIZE")])
annual_long <- annual_long[, c("CONSUMPTION_SEGMENT_CODE", "ID_NAME",
                                  "Category1", "Category2", "WEIGHT\\SIZE", "Date", "Percentage")]

# Write to Excel
wb <- loadWorkbook("datadownload.xlsx")
addWorksheet_safe <- function(wb, name) {
  if (name %in% names(wb)) removeWorksheet(wb, name)
  addWorksheet(wb, name)
}

addWorksheet_safe(wb, "Metadata")
writeData(wb, "Metadata", meta_sheet)

addWorksheet_safe(wb, "Chained")
writeData(wb, "Chained", chained_long)

addWorksheet_safe(wb, "Average price")
writeData(wb, "Average price", avgprice_long)

addWorksheet_safe(wb, "Monthly growth")
writeData(wb, "Monthly growth", monthly_long)

addWorksheet_safe(wb, "Annual growth")
writeData(wb, "Annual growth", annual_long)

saveWorkbook(wb, "datadownload.xlsx", overwrite = TRUE)
cat("Saved: datadownload.xlsx\n")
```

```
## Saved: datadownload.xlsx
```

# Compare results with Python output

The `py_*.csv` files are the outputs generated by `manual-process-run.py` (the Python
equivalent of `manual-process.ipynb`). Run that script first to produce them.


``` r
numeric_compare <- function(py_file, r_file, label, id_cols = 1) {
  if (!file.exists(py_file) || !file.exists(r_file)) {
    cat(label, ": file(s) missing – run manual-process-run.py first\n")
    return(invisible(NULL))
  }
  py <- read.csv(py_file, check.names = FALSE, stringsAsFactors = FALSE)
  r  <- read.csv(r_file,  check.names = FALSE, stringsAsFactors = FALSE)

  # Sort by first ID column so row order doesn't affect comparison
  py <- py[order(py[[1]]), ]
  r  <- r[order(r[[1]]), ]

  cat("\n===", label, "===\n")
  cat("Dims  Python:", nrow(py), "x", ncol(py), "\n")
  cat("Dims  R     :", nrow(r),  "x", ncol(r),  "\n")
  if (!identical(names(py), names(r))) cat("Column names: DIFFER ✗\n") else cat("Column names: match ✓\n")

  # Numeric comparison
  val_cols <- names(py)[-(1:id_cols)]
  py_num   <- suppressWarnings(sapply(py[, val_cols, drop = FALSE], as.numeric))
  r_num    <- suppressWarnings(sapply(r[,  val_cols, drop = FALSE], as.numeric))

  diffs       <- abs(py_num - r_num)
  max_diff    <- max(diffs, na.rm = TRUE)
  na_py_only  <- sum(is.na(py_num) & !is.na(r_num))
  na_r_only   <- sum(!is.na(py_num) & is.na(r_num))

  cat("Max numeric diff:", round(max_diff, 6), "\n")
  cat("NA mismatches   : py-only =", na_py_only, "| r-only =", na_r_only, "\n")

  if (max_diff <= 0.001 && na_py_only == 0 && na_r_only == 0) {
    cat("RESULT: MATCH ✓ (diff within rounding tolerance)\n")
  } else {
    cat("RESULT: DIFFERENCES FOUND ✗\n")
    idx <- which(diffs > 0.001, arr.ind = TRUE)
    if (nrow(idx) > 0) {
      cat("Sample diffs:\n")
      for (i in seq_len(min(5L, nrow(idx)))) {
        ri <- idx[i, 1]; ci <- idx[i, 2]
        cat("  row", ri, val_cols[ci], "->",
            "py:", py_num[ri, ci], "| r:", r_num[ri, ci], "\n")
      }
    }
  }
}

numeric_compare("py_chained.csv",               "chained.csv",               "CHAINED",        1)
```

```
## 
## === CHAINED ===
## Dims  Python: 53 x 52 
## Dims  R     : 53 x 52 
## Column names: match ✓
## Max numeric diff: 0.001 
## NA mismatches   : py-only = 0 | r-only = 0 
## RESULT: MATCH ✓ (diff within rounding tolerance)
```

``` r
numeric_compare("py_unchained.csv",             "unchained.csv",             "UNCHAINED",      1)
```

```
## 
## === UNCHAINED ===
## Dims  Python: 53 x 52 
## Dims  R     : 53 x 52 
## Column names: match ✓
## Max numeric diff: 0 
## NA mismatches   : py-only = 0 | r-only = 0 
## RESULT: MATCH ✓ (diff within rounding tolerance)
```

``` r
numeric_compare("py_avgprice_merged.csv",       "avgprice_merged.csv",       "AVGPRICE",       2)
```

```
## 
## === AVGPRICE ===
## Dims  Python: 70 x 53 
## Dims  R     : 70 x 53 
## Column names: match ✓
## Max numeric diff: 0 
## NA mismatches   : py-only = 0 | r-only = 0 
## RESULT: MATCH ✓ (diff within rounding tolerance)
```

``` r
numeric_compare("py_monthly_growth_merged.csv", "monthly_growth_merged.csv", "MONTHLY GROWTH", 2)
```

```
## 
## === MONTHLY GROWTH ===
## Dims  Python: 70 x 52 
## Dims  R     : 70 x 52 
## Column names: match ✓
## Max numeric diff: 0 
## NA mismatches   : py-only = 0 | r-only = 0 
## RESULT: MATCH ✓ (diff within rounding tolerance)
```

``` r
numeric_compare("py_annual_growth_merged.csv",  "annual_growth_merged.csv",  "ANNUAL GROWTH",  2)
```

```
## 
## === ANNUAL GROWTH ===
## Dims  Python: 70 x 41 
## Dims  R     : 70 x 41 
## Column names: match ✓
## Max numeric diff: 0 
## NA mismatches   : py-only = 0 | r-only = 0 
## RESULT: MATCH ✓ (diff within rounding tolerance)
```
