detach("package:tidyorgs", unload = TRUE)

devtools::install_local("C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring GH/sectoring/tidyorgs-main/tidyorgs-main", force = TRUE)
devtools::install_local("C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring GH/sectoring/diverstidy-main/diverstidy-main", force = TRUE)

library(tidyorgs)
library(diverstidy)
library(tibble)
library(dplyr)
library(rlang)
library(tidyverse)
library(readxl)
library(writexl)
options(width = 1000)
library(arrow)

data <- read_parquet('C:\\Users\\Saluja_R\\OneDrive - Westat\\Desktop\\Westat\\OSS\\Sectoring GH\\sectoring\\Code\\user_data_2024_12_04.parquet')

# # Step 1: Remove empty columns
# data_cleaned <- data %>%
#   select(where(~ !all(is.na(.))))  # Removes columns where all values are NA

# # Step 2: Replace "NA" strings with actual NA values across all columns
# # Apply na_if("NA") only to character columns
# data_cleaned <- data_cleaned %>%
#   mutate(across(where(is.character), ~ na_if(., "NA")))

# # Step 3: Check for any other anomalies in columns like company or email
# # Ensure company and email are character vectors
# data_cleaned <- data_cleaned %>%
#   mutate(across(c(company, author_email), as.character))

# data_cleaned <- data_cleaned %>%
#   select(where(~ !all(is.na(.) | . == "")))

# data_cleaned %>% 
#   filter(is.na(company) | company == "" | is.na(author_email) | author_email == "")

# str(data_cleaned$company)
# str(data_cleaned$author_email)
# Step 4: Run your function with cleaned data
classified_sectors <- data %>%
  detect_academic(login, company, organization, email = author_email)

classified_sectors <- classified_sectors %>%
  detect_business(login, company, organization1, email = author_email)

classified_sectors <- classified_sectors %>%
  detect_government(login, company, organization2, email = author_email)

classified_sectors <- classified_sectors %>%
  detect_nonprofit(login, company, organization3, email = author_email)


# Step 5: Save the output
write_xlsx(classified_sectors, "C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring GH/sectoring/Code/user_data_2024_12_04_allSectors.xlsx")


