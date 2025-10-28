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


# Specify path to input and output data here

path_to_user_data      <- file.path("Data", "user_data_2025_02_11.parquet")
path_to_sector_output  <- file.path("Data", "user_data_sectors_2025_03_31.parquet")
path_to_country_output <- file.path("Data", "Partitioned_Output", "user_data_combined.parquet")
path_to_output         <- file.path("Data", "user_data_country_sectors_governmentClassification_2025_03_24.parquet")

#--------------------------------------------------------------------
# Checking total number of rows and duplicate values in user data
#--------------------------------------------------------------------

data_user <- read_parquet(path_to_user_data)
sum(duplicated(data_user$login)) # 0 - that means no duplicate logins
nrow(data_user) # total number of users 

# Count missing values and filter rows with the least missing values per login
data_user$missing_count <- rowSums(is.na(data_user))

# Order by login, missing_count (ascending)
data_user <- data_user[order(data_user$login, data_user$missing_count), ]

# Remove duplicates while keeping the first occurrence (least missing values or 'Germany' priority)
data_user_unique <- data_user[!duplicated(data_user$login), ]

# Drop the extra column
data_user_unique$missing_count <- NULL

# Collapsing country columns -- as some user might have been assigned two countries and therefore are appearing as duplicate logins

colnames(data_user_unique)
# Step 1: Collapse country columns

country_collapsed <- data_country |>
  group_by(login) |>
  summarize(
    across(c(country_email, country_bio, country_location, country_socialaccounts),
           function(x) str_c(unique(x[!is.na(x)]), collapse = " | ")),
    .groups = "drop"
  )

# Step 2: Get the non-country columns

non_country_columns <- data_country |>
  select(-country_email, -country_bio, -country_location, -country_socialaccounts) |>
  distinct(login, .keep_all = TRUE)

# Step 3: Join the collapsed country columns back
data_country_unique <- non_country_columns %>%
  left_join(country_collapsed, by = "login")

sum(duplicated(data_user_unique_updated$login)) # check 


#--------------------------------------------------------------------
# Additional Cleaning for Sectoring File
#--------------------------------------------------------------------


columns_to_check <- c("organization_company_academic", "academic", "country_academic", "organization_email_academic",
                      "organization_company_business", "business", "country_business", "organization_email_business",
                      "organization_company_government", "government", "country_government", "organization_email_government",
                      "organization_company_nonprofit", "nonprofit", "country_nonprofit", "organization_email_nonprofit")


na_pipe_rows <- data_user_unique_updated %>%
  filter(if_any(all_of(columns_to_check), ~ . == "NA|NA"))

data_sector_unique <- data_sector_unique %>%
  mutate(across(all_of(columns_to_check), ~ ifelse(. == "NA|NA", NA, .)))

na_pipe_rows_triple <- data_user_unique_updated %>%
  filter(if_any(all_of(columns_to_check), ~ . == "NA|NA|NA"))

data_sector_unique <- data_sector_unique %>%
  mutate(across(all_of(columns_to_check), ~ ifelse(. == "NA|NA|NA", NA, .)))

# Adding condition for GitHub employees 
data_user_unique_updated <- data_user_unique_updated %>%
  mutate(
    business = if_else(isemployee, 1, business),
    organization_company_business = if_else(isemployee, "GitHub", organization_company_business),
    country_business = if_else(isemployee, "United States", country_business)
  )

#--------------------------------------------------------------------
# Additional Cleaning for Country File
#--------------------------------------------------------------------

# Country data cleaning
state_codes <- c("al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky", 
                 "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", 
                 "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", 
                 "wv", "wi", "wy", "nyc", "dc", "sf")

# PROCESS 1: If the last two letters correspond to state abbreviation, then classify as US

data_country_unique <- data_country_unique %>%
  mutate(
    location = tolower(location), # Ensure lowercase for comparison
    country_location = case_when(
      is.na(location)                               ~ country_location,
      location == "nothing"                         ~ country_location,
      !str_detect(
        tolower(country_location), 
        fixed("united states", ignore_case = TRUE)
       )                                            ~ country_location,
      str_sub(location, -2, -1) %in% state_codes    ~ "United States", 
      TRUE                                          ~ country_location
    )
  )

# PROCESS 2: Remove all NAs from the country column

columns_to_modify <- c("country_location", "country_bio", "country_socialaccounts", "country_email")

data_user_unique_updated <- data_user_unique_updated %>%
  mutate(across(all_of(columns_to_modify), ~ if_else(
    str_count(., "\\|") == 1 & str_detect(., "NA"),
    str_replace_all(., "\\|?NA\\|?", ""),  # Step 1: Remove single "NA" and "|"
    .
  ))) %>%
  mutate(across(all_of(columns_to_modify), ~ if_else(
    str_count(., "\\|") > 1,
    str_replace_all(str_replace_all(., "\\|NA", ""), "NA\\|", ""),  # Step 2: Remove "|NA" and "NA|"
    .
  )))


# PROCESS 3: Correct specific misclassifications

data_country_unique <- data_country_unique %>%
  mutate(
    country_location = case_when(
      str_detect(tolower(location), "new jersey") ~ "United States", # Avoid misclassification of New Jersey as the island nation of Jersey 
      str_detect(tolower(location), "new mexico") ~ "United States", # Avoid misclassification of New Mexico as Mexico
      str_detect(tolower(location), " usa")       ~ "United States", # Make sure US has a consistent name throughout
      TRUE                                        ~ country_location
    )
  )

#--------------------------------------------------------------------
# Merging the output from diverstidy and tidyorgs
#--------------------------------------------------------------------

data_country_unique <- data_country_unique %>%
  select(login, country_location, country_bio, country_socialaccounts, country_email)

merged_data <- full_join(data_sector_unique, data_country_unique, by = "login")

write_parquet(merged_data, sink = path_to_output)
