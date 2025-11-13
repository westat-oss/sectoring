# Specify display options, load necessary packages

options(width = 1000)

library(tibble)
library(dplyr)
library(rlang)
library(stringr)
library(tidyverse)
library(readxl)
library(writexl)
library(arrow)
library(glue)

#--------------------------------------------------------------------
# Load input datasets
#--------------------------------------------------------------------

path_to_user_data      <- file.path("Data", "user_data_2025_10_08.parquet")
path_to_sector_output  <- file.path("Data", "user_data_sectors_2025_10_08.parquet")
path_to_country_output <- file.path("Data", "Partitioned_Output_2025_10_08", "user_data_combined.parquet")
path_to_output         <- file.path("Data", "user_data_country_sectors_2025_10_08.parquet")

data_user    <- read_parquet(path_to_user_data)
data_sector  <- read_parquet(path_to_sector_output)
data_country <- read_parquet(path_to_country_output)

#--------------------------------------------------------------------
# Checking total number of rows and duplicate values in datasets
#--------------------------------------------------------------------

data_user_record_counts <- data_user |> 
  summarize(n_logins = n_distinct(login),
            n_rows   = n()) |>
  collect()
data_sector_record_counts <- data_sector |> 
  summarize(n_logins = n_distinct(login),
            n_rows   = n()) |>
  collect()
data_country_record_counts <- data_country |> 
  summarize(n_logins = n_distinct(login),
            n_rows   = n()) |>
  collect()

message(
  glue("`data_user` has {data_user_record_counts$n_rows} records, and {data_user_record_counts$n_logins} unique login IDs.")
)
message(
  glue("`data_sector` has {data_sector_record_counts$n_rows} records, and {data_sector_record_counts$n_logins} unique login IDs.")
)
message(
  glue("`data_country` has {data_country_record_counts$n_rows} records, and {data_country_record_counts$n_logins} unique login IDs.")
)

#--------------------------------------------------------------------
# For each login, collapse country assignments from multiple rows into a single entry
#--------------------------------------------------------------------

# Collapsing country columns -- as some users might have been assigned multiple countries and therefore are appearing as duplicate logins

data_country_unique <- data_country |>
  select(login, starts_with("country_")) |>
  collect() |>
  group_by(login) |>
  summarize(
    across(
      starts_with("country_"),
      function(x) {
        x[!is.na(x)] |>
          unique() |>
          setdiff("NA") |>
          str_c(collapse = " | ")
      }
    ),
    .groups = "drop"
  )
data_country_unique <- data_country_unique[ , !(names(data_country_unique) %in%
  c("country_academic", "country_business", "country_government", "country_nonprofit"))]

#--------------------------------------------------------------------
# Additional Cleaning for Sectoring File
#--------------------------------------------------------------------

sectoring_columns <- setdiff(colnames(data_sector), colnames(data_user))

# For a sectoring column with only entries of "NA", convert to explicit missing values
data_sector_cleaned <- data_sector %>%
  select(login, isemployee, one_of(sectoring_columns)) |>
  collect() |>
  mutate(across(all_of(sectoring_columns), function(x) {
    ifelse(str_detect(x, "^(NA|\\|)+$"), NA_character_, x)
  }))

# GitHub employees are flagged as business cases
# with their corresponding business organization assignment set to 'GitHub'
# and their corresponding business country assignment set to 'United States'
data_sector_cleaned <- data_sector_cleaned %>%
  mutate(
    business = if_else(isemployee, 1, business),
    organization_company_business = if_else(isemployee, "GitHub", organization_company_business),
    country_business = if_else(isemployee, "United States", country_business)
  )

data_sector_cleaned_unique <- data_sector_cleaned |>
  distinct(login, .keep_all = TRUE)

#--------------------------------------------------------------------
# Merging the output from diverstidy and tidyorgs
#--------------------------------------------------------------------

merged_data <- full_join(
  x = data_sector_cleaned_unique, 
  y = data_country_unique |>
    select(login, starts_with("country_")) |>
    collect(), 
  by = "login",
  relationship = "one-to-one"
)

x_add <- data_user |>
  select(login, all_of(other_cols_from_data_user)) |>
  distinct(login, .keep_all = TRUE) 

other_cols_from_data_user <- setdiff(colnames(x_add), colnames(merged_data))
if (length(other_cols_from_data_user) > 0) {
  merged_data <- left_join(
    x  = x_add |> 
      select(login, all_of(other_cols_from_data_user)) |>
      collect(),
    y  = merged_data,
    by = "login",
    relationship = "one-to-one",
    unmatched = "error"
  )
}

if (nrow(merged_data) != nrow(data_user)) {
  stop("`merged_data` should have the same number of records as `data_user`.")
}

#--------------------------------------------------------------------
# Custom Country Names
#--------------------------------------------------------------------

country_cols <- grep("^country_", names(merged_data), value = TRUE)

# define replacement mapping
replacements <- c(
  "Czech Republic" = "Czechia",
  "Hong Kong" = "China"
)

# apply replacements across all country_* columns
merged_data_replaced <- merged_data %>%
  mutate(across(all_of(country_cols),
                ~ dplyr::recode(., !!!replacements)))

write_parquet(merged_data_replaced, sink = path_to_output)


