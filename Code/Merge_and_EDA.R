library(tibble)
library(dplyr)
library(rlang)
library(stringr)
library(tidyverse)
library(readxl)
library(writexl)
options(width = 1000)
library(arrow)


# Specify path to input and output data here
path_to_user_data <- 'Data\\'
path_to_sector_output <- 'Data\\'
path_to_country_output <- 'Data\\'
path_to_output <- 'Data\\'

#--------------------------------------------------------------------
# Checking total number of rows and duplicate values in user data
#--------------------------------------------------------------------

data_user <- read_parquet(path_to_user_data)
sum(duplicated(data_user$login)) # 0 - that means no duplicate logins
nrow(data_user) # total number of users - 5930379


#--------------------------------------------------------------------
# Checking total number of rows and duplicate values in sectoring data
#--------------------------------------------------------------------
data_sector <- read_parquet(path_to_sector_output)
sum(duplicated(data_sector$login)) 
nrow(data_sector)

# If duplicate values exist in sector output

# Creating file with duplicate values to manually inspect
duplicated_logins_sector <- data_sector %>%
  filter(duplicated(login) | login %in% login[duplicated(login)])

write.csv(duplicated_logins_sector, "duplicated_logins_inSectorFile.csv", row.names = FALSE)

# Count missing values and filter rows with the least missing values per login
data_sector$missing_count <- rowSums(is.na(data_sector))

# Order by login, missing_count (ascending)
data_sector <- data_sector[order(data_sector$login, data_sector$missing_count), ]

# Remove duplicates while keeping the first occurrence (least missing values or 'Germany' priority)
data_sector_unique <- data_sector[!duplicated(data_sector$login), ]

# Drop the extra column
data_sector_unique$missing_count <- NULL

#--------------------------------------------------------------------
# Checking total number of rows and duplicate values in country data
#--------------------------------------------------------------------
data_country <- read_parquet(path_to_country_output)
sum(duplicated(data_country$login))
nrow(data_country)

# If duplicate data exists in country output 

# Creating file with duplicate values to manually inspect
duplicated_logins_country <- data_country %>%
  filter(duplicated(login) | login %in% login[duplicated(login)])

write.csv(duplicated_logins_country, "duplicated_logins_inCountryFile.csv", row.names = FALSE)

# Collapsing country columns -- as some user might have been assigned two countries and therefore are appearing as duplicate logins

# Step 1: Collapse country columns
country_collapsed <- data_country %>%
  group_by(login) %>%
  summarise(
    country_email = str_c(unique(na.omit(country_email)), collapse = " | "),
    country_bio = str_c(unique(na.omit(country_bio)), collapse = " | "),
    country_location = str_c(unique(na.omit(country_location)), collapse = " | "),
    country_socialaccounts = str_c(unique(na.omit(country_socialaccounts)), collapse = " | "),
    .groups = "drop"
  )

# Step 2: Select one representative row per login from original data (e.g., first row)
non_country_columns <- data_country %>%
  group_by(login) %>%
  slice(1) %>%
  select(-country_email, -country_bio, -country_location, -country_socialaccounts)

# Step 3: Join the collapsed country columns back
data_country_unique <- data_country %>%
  left_join(country_collapsed, by = "login")

sum(duplicated(data_country_unique$login)) # check 


#--------------------------------------------------------------------
# Additional Cleaning for Sectoring File
#--------------------------------------------------------------------


columns_to_check <- c("organization_company_academic", "academic", "country_academic", "organization_email_academic",
                      "organization_company_business", "business", "country_business", "organization_email_business",
                      "organization_company_government", "government", "country_government", "organization_email_government",
                      "organization_company_nonprofit", "nonprofit", "country_nonprofit", "organization_email_nonprofit")


na_pipe_rows <- data_sector_unique %>%
  filter(if_any(all_of(columns_to_check), ~ . == "NA|NA"))

# nrow(na_pipe_rows)

data_sector_unique <- data_sector_unique %>%
  mutate(across(all_of(columns_to_check), ~ ifelse(. == "NA|NA", NA, .)))

na_pipe_rows_triple <- data_sector_unique %>%
  filter(if_any(all_of(columns_to_check), ~ . == "NA|NA|NA"))

# nrow(na_pipe_rows_triple) 

data_sector_unique <- data_sector_unique %>%
  mutate(across(all_of(columns_to_check), ~ ifelse(. == "NA|NA|NA", NA, .)))

# Adding condition for GitHub employees 
data_sector_unique <- data_sector_unique %>%
  mutate(
    business = if_else(isemployee == TRUE, 1, business),
    organization_company_business = if_else(isemployee == TRUE, "GitHub", organization_company_business),
    country_business = if_else(isemployee == TRUE, "United States", country_business)
  )

# save the cleaned version 
# write_parquet(data_sector_unique, sink = 'Data\\user_data_sectors_2025_03_26_codegov_unique.parquet')
# data_sector_unique <- read_parquet('Code\\user_data_sectors_2025_04_03.parquet')

#--------------------------------------------------------------------
# Additional Cleaning for Country File
#--------------------------------------------------------------------

# data_country <- read_parquet('Data\\user_data_country_2025_03_04.parquet')

# Remove duplicate columns
# data_country_unique <- data_country_unique  %>%
#   distinct(login, .keep_all = TRUE)

# nrow(data_country_unique)

# Country data cleaning
state_codes <- c("al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky", 
                 "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", 
                 "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", 
                 "wv", "wi", "wy", "nyc", "dc", "sf")

# PROCESS 1: If the last two letters correspond to state abbreviation - then US
data_country_unique <- data_country_unique %>%
  mutate(location = tolower(location), # Ensure lowercase for comparison
         country_location = case_when(
           !is.na(location) & location != "nothing" &  # location should not be null or nothing
           substr(location, nchar(location)-1, nchar(location)) %in% state_codes &  # checking if the last two characters correspond to a state abbreviation
           str_detect(tolower(country_location), fixed("united states", ignore_case = TRUE)) ~ "United States", # checking if the country is United States - detected already by the package
           TRUE ~ country_location
         ))

# PROCESS 2: Remove all NAs from the country column
columns_to_modify <- c("country_location", "country_bio", "country_socialaccounts", "country_email")


data_country_unique <- data_country_unique %>%
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


# PROCESS 3: If the location is New Jersey then it is United States and not Jersey
data_country_unique <- data_country_unique %>%
    mutate(
            country_location = if_else(
            str_detect(tolower(location), "new jersey"),
            "United States",
            country_location
            )
    )

# PROCESS 4: If the location is New Mexico then it is United States and not Mexico
data_country_unique <- data_country_unique %>%
    mutate(
            country_location = if_else(
            str_detect(tolower(location), "new mexico"),
            "United States",
            country_location
            )
    )
  
# PROCESS 5: Making sure US has a consistent name throughout 
data_country_unique <- data_country_unique %>%
    mutate(
            country_location = if_else(
            str_detect(tolower(location), " usa"),
            "United States",
            country_location
            )
    )
# write.csv(data_country_unique, "coutry_codegov.csv", row.names = FALSE)
# save the cleaned version 
# write_parquet(data_country_unique, sink = 'Code\\user_data_country_2025_04_09_codegov_country.parquet')

# data_country_unique <- read_parquet('Data\\user_data_country_2025_03_04.parquet')


#--------------------------------------------------------------------
# Merging the output from diverstidy and tidyorgs
#--------------------------------------------------------------------

data_country_unique <- data_country_unique %>%
  select(login, country_location, country_bio, country_socialaccounts, country_email)


merged_data <- full_join(data_sector_unique, data_country_unique, by = "login")


# colnames(merged_data)
write_parquet(merged_data, sink = path_to_output)
# nrow(merged_data)
# write.csv(merged_data, "final_codegov.csv", row.names = FALSE)


#--------------------------------------------------------------------
# EDA 
#--------------------------------------------------------------------

# Total number of users (excluding NAs for consistency)
total_users <- nrow(merged_data)

# Count users who have at least one non-NA value in the specified columns
users_with_country_data <- merged_data %>%
  filter(!is.na(country_location) | 
         !is.na(country_email) | 
         !is.na(country_academic) | 
         !is.na(country_government)) %>%
  nrow()

# Calculate percentage
percentage_with_country_data <- (users_with_country_data / total_users) * 100

# Print the result
cat("Percentage of users with country data:", round(percentage_with_country_data, 2), "%\n")

# Users assigned country through location
users_location <- merged_data %>%
  filter(!is.na(country_location)) %>%
  nrow()

# Users assigned country through email (excluding those already assigned through location)
users_email <- merged_data %>%
  filter(is.na(country_location) & !is.na(country_email)) %>%
  nrow()

# Users assigned country through academic (excluding those already assigned through location and email)
users_academic <- merged_data %>%
  filter(is.na(country_location) & is.na(country_email) & !is.na(country_academic)) %>%
  nrow()

# Users assigned country through government (excluding all previous categories)
users_government <- merged_data %>%
  filter(is.na(country_location) & is.na(country_email) & is.na(country_academic) & !is.na(country_government)) %>%
  nrow()

# Compute cumulative percentages
location_pct <- (users_location / total_users) * 100
email_cumulative_pct <- ((users_location + users_email) / total_users) * 100
academic_cumulative_pct <- ((users_location + users_email + users_academic) / total_users) * 100
government_cumulative_pct <- ((users_location + users_email + users_academic + users_government) / total_users) * 100

# Print results
cat("Percentage of users assigned country through location:", round(location_pct, 2), "%\n")
cat("Percentage of users assigned country through email (cumulative):", round(email_cumulative_pct, 2), "%\n")
cat("Percentage of users assigned country through academic (cumulative):", round(academic_cumulative_pct, 2), "%\n")
cat("Percentage of users assigned country through government (cumulative):", round(government_cumulative_pct, 2), "%\n")

email_diff <- email_cumulative_pct - location_pct
academic_diff <- academic_cumulative_pct - email_cumulative_pct
government_diff <- government_cumulative_pct - academic_cumulative_pct

# Print results with differences
cat("Percentage of users assigned country through location:", round(location_pct, 2), "%\n")
cat("Additional percentage assigned through email:", round(email_diff, 2), "%\n")
cat("Additional percentage assigned through academic:", round(academic_diff, 2), "%\n")
cat("Additional percentage assigned through government:", round(government_diff, 2), "%\n")





# Percentage of users with no 'company' information (either NA or empty)
no_company_pct <- sum(is.na(merged_data$company) | merged_data$company == "" | tolower(merged_data$company) == "nothing") / total_users * 100

# Percentage of users in each sector
academic_pct <- sum(merged_data$academic == 1, na.rm = TRUE) / total_users * 100
business_pct <- sum(merged_data$business == 1, na.rm = TRUE) / total_users * 100
government_pct <- sum(merged_data$government == 1, na.rm = TRUE) / total_users * 100
nonprofit_pct <- sum(merged_data$nonprofit == 1, na.rm = TRUE) / total_users * 100

# Print the results
cat("Percentage of users with no 'company' information:", round(no_company_pct, 2), "%\n")
cat("Percentage of users classified in 'academic' sector:", round(academic_pct, 2), "%\n")
cat("Percentage of users classified in 'business' sector:", round(business_pct, 2), "%\n")
cat("Percentage of users classified in 'government' sector:", round(government_pct, 2), "%\n")
cat("Percentage of users classified in 'nonprofit' sector:", round(nonprofit_pct, 2), "%\n")


location_country_pct <- sum(!is.na(merged_data$country_location), na.rm = TRUE) / total_users * 100
email_country_pct <- sum(!is.na(merged_data$country_email), na.rm = TRUE) / total_users * 100
bio_country_pct <- sum(!is.na(merged_data$country_bio), na.rm = TRUE) / total_users * 100
socialaccounts_country_pct <- sum(!is.na(merged_data$country_socialaccounts), na.rm = TRUE) / total_users * 100

cat("Total percentage of users assigned country through location:", round(location_country_pct, 2), "%\n")
cat("Total percentage of users assigned country through email:", round(email_country_pct, 2), "%\n")
cat("Total percentage of users assigned country through bio:", round(bio_country_pct, 2), "%\n")
cat("Total percentage of users assigned country through social accounts:", round(socialaccounts_country_pct, 2), "%\n")

academic_country_pct <- sum(!is.na(merged_data$country_academic), na.rm = TRUE) / total_users * 100
business_country_pct <- sum(!is.na(merged_data$country_business), na.rm = TRUE) / total_users * 100
government_country_pct <- sum(!is.na(merged_data$country_government), na.rm = TRUE) / total_users * 100
nonprofit_country_pct <- sum(!is.na(merged_data$country_nonprofit), na.rm = TRUE) / total_users * 100

# Print the results
cat("Total percentage of users assigned country through academic sector:", round(academic_country_pct, 2), "%\n")
cat("Total percentage of users assigned country through business sector:", round(business_country_pct, 2), "%\n")
cat("Total percentage of users assigned country through government sector:", round(government_country_pct, 2), "%\n")
cat("Total percentage of users assigned country through nonprofit sector:", round(nonprofit_country_pct, 2), "%\n")




# Step 1: Create a new column `is_US` based on country fields
merged_data <- merged_data %>%
  mutate(is_US = if_else(
    str_detect(country_location, "United States") | 
    str_detect(country_email, "United States") |
    str_detect(country_academic, "United States") | 
    str_detect(country_government, "United States"),
    TRUE, FALSE, missing = FALSE
  ))

us_location <- merged_data %>%
  filter(str_detect(country_location, "United States")) %>%
  nrow()

# Users assigned US through email (excluding those already assigned through location)
us_email <- merged_data %>%
  filter(is.na(country_location) & str_detect(country_email, "United States")) %>%
  nrow()

# Users assigned US through academic (excluding those already assigned through location and email)
us_academic <- merged_data %>%
  filter(is.na(country_location) & is.na(country_email) & str_detect(country_academic, "United States")) %>%
  nrow()

# Users assigned US through government (excluding all previous categories)
us_government <- merged_data %>%
  filter(is.na(country_location) & is.na(country_email) & is.na(country_academic) & str_detect(country_government, "United States")) %>%
  nrow()

# Compute cumulative percentages
us_location_pct <- (us_location / total_users) * 100
us_email_cumulative_pct <- ((us_location + us_email) / total_users) * 100
us_academic_cumulative_pct <- ((us_location + us_email + us_academic) / total_users) * 100
us_government_cumulative_pct <- ((us_location + us_email + us_academic + us_government) / total_users) * 100

# Compute differences in cumulative percentages
us_email_diff <- us_email_cumulative_pct - us_location_pct
us_academic_diff <- us_academic_cumulative_pct - us_email_cumulative_pct
us_government_diff <- us_government_cumulative_pct - us_academic_cumulative_pct

# Print results
cat("Percentage of users assigned 'United States' through location:", round(us_location_pct, 2), "%\n")
cat("Additional percentage assigned 'United States' through email:", round(us_email_diff, 2), "%\n")
cat("Additional percentage assigned 'United States' through academic:", round(us_academic_diff, 2), "%\n")
cat("Additional percentage assigned 'United States' through government:", round(us_government_diff, 2), "%\n")


# Step 2: Calculate the percentage of users from the US
total_users <- nrow(merged_data)
us_users <- sum(merged_data$is_US, na.rm = TRUE)
us_users_pct <- (us_users / total_users) * 100

# Step 3: Calculate sector percentages for US users
us_data <- merged_data %>% filter(is_US == TRUE)

us_academic_pct <- sum(us_data$academic == 1, na.rm = TRUE) / us_users * 100
us_business_pct <- sum(us_data$business == 1, na.rm = TRUE) / us_users * 100
us_government_pct <- sum(us_data$government == 1, na.rm = TRUE) / us_users * 100
us_nonprofit_pct <- sum(us_data$nonprofit == 1, na.rm = TRUE) / us_users * 100

# Print the results
cat("Percentage of users assigned 'United States':", round(us_users_pct, 2), "%\n\n")

cat("For users assigned 'United States':\n")
cat("Percentage classified in 'academic' sector:", round(us_academic_pct, 2), "%\n")
cat("Percentage classified in 'business' sector:", round(us_business_pct, 2), "%\n")
cat("Percentage classified in 'government' sector:", round(us_government_pct, 2), "%\n")
cat("Percentage classified in 'nonprofit' sector:", round(us_nonprofit_pct, 2), "%\n")


head(merged_data)
# Percentage of users with no 'company' information (either NA or empty)
no_location_pct <- sum(is.na(merged_data$location) | merged_data$location == "" | tolower(merged_data$location) == "nothing") / total_users * 100
no_location_pct


sample_data <- merged_data %>% sample_n(1000)

# Save the sample to a CSV file
write.csv(sample_data, "sample_merged_data.csv", row.names = FALSE)

# Confirm the sample was created
cat("Sample of 1000 rows saved as 'sample_merged_data.csv'\n")




data_user <- read_parquet('Code\\user_data_country_sectors_2025_03_10.parquet')
colnames(data_user)

counts <- data_user %>%
  filter(country_location == "United States") %>%
  group_by(organization_company_government) %>%
  summarise(count = n()) %>%
  arrange(desc(count))

