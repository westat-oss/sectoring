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

# Load data ----

path_to_output <- file.path("Data", "user_data_country_sectors_2025_10_08.parquet")
merged_data <- read_parquet(path_to_output)

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