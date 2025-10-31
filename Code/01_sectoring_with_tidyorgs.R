# Specify display options, load necessary packages ----
options(width = 1000)

library(tidyorgs)
library(diverstidy)
library(tibble)
library(dplyr)
library(rlang)
library(tidyverse)
library(readxl)
library(writexl)
library(arrow)
library(progress)

# Specify relevant paths for data ----

path_to_user_data <- file.path("Data", "user_data_diff_sample_2025_10_08.parquet")
path_to_output    <- file.path("Data", "user_data_sectors_2025_10_08.parquet")

# Load user data ----

data <- read_parquet(path_to_user_data)

# Run sectoring functions from tidyorgs ----

## Academic

classified_sectors <- data %>%
  detect_academic(id = login, 
                  input = company, 
                  output = organization_company_academic, 
                  output_email = organization_email_academic, 
                  country = TRUE, 
                  email = author_email)

classified_sectors <- classified_sectors %>%
    rename(country_academic = country)

## Business

classified_sectors <- classified_sectors %>%
  detect_business(id = login, 
                  input = company, 
                  output = organization_company_business, 
                  output_email = organization_email_business, 
                  country = TRUE, 
                  email = author_email)

classified_sectors <- classified_sectors %>%
    rename(country_business = country)

## Government

classified_sectors <- classified_sectors %>%
  detect_government(id = login, 
                    input = company, 
                    output = organization_company_government, 
                    output_email = organization_email_government,
                    country = TRUE, 
                    email = author_email)

classified_sectors <- classified_sectors %>%
    rename(country_government = country)

## Nonprofit

classified_sectors <- classified_sectors %>%
  detect_nonprofit(id = login, 
                  input = company,
                  output = organization_company_nonprofit,
                  output_email = organization_email_nonprofit,
                  country = TRUE, 
                  email = author_email)

classified_sectors <- classified_sectors %>%
    rename(country_nonprofit = country)

# Save the output file ----

write_parquet(classified_sectors, sink = path_to_output)
