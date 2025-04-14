# install.packages("devtools")
# install.packages("arrow")
# install.packages("rlang")
# install.packages("stringr")
# install.packages("tibble")
# install.packages("tidyverse")
# install.packages("readxl")
# install.packages("writexl")
# install.packages("dplyr")

# chnage the paths to input and outpt files here
path_to_tidyorgs           <- "tidyorgs-main/tidyorgs-main"
path_to_diverstidy         <- "diverstidy-main/diverstidy-main"
path_to_user_data          <- "Data/user_data_2025_02_11.parquet"
path_to_output             <- "Data/user_data_sectors_2025_03_31.parquet"

# if your current env already has these packages please detach them before running the script again (this will update the packages in your env)
# detach("package:tidyorgs", unload = TRUE)
# detach("package:diverstidy", unload = TRUE)

devtools::install_local(path_to_tidyorgs, force = TRUE)
devtools::install_local(path_to_diverstidy, force = TRUE)

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


data <- read_parquet(path_to_user_data)
nrow(data)

classified_sectors <- data %>%
  detect_academic(id = login, 
                  input = company, 
                  output = organization_company_academic, 
                  output_email = organization_email_academic, 
                  country = TRUE, 
                  email = author_email)

classified_sectors <- classified_sectors %>%
    rename(country_academic = country)

classified_sectors <- classified_sectors %>%
  detect_business(id = login, 
                  input = company, 
                  output = organization_company_business, 
                  output_email = organization_email_business, 
                  country = TRUE, 
                  email = author_email)

classified_sectors <- classified_sectors %>%
    rename(country_business = country)

classified_sectors <- classified_sectors %>%
  detect_government(id = login, 
                    input = company, 
                    output = organization_company_government, 
                    output_email = organization_email_government,
                    country = TRUE, 
                    email = author_email)


classified_sectors <- classified_sectors %>%
    rename(country_government = country)

classified_sectors <- classified_sectors %>%
  detect_nonprofit(id = login, 
                  input = company,
                  output = organization_company_nonprofit,
                  output_email = organization_email_nonprofit,
                  country = TRUE, 
                  email = author_email)

classified_sectors <- classified_sectors %>%
    rename(country_nonprofit = country)


# For testing:
# nrow(classified_sectors)
# colnames(classified_sectors)
# write.csv(classified_sectors, "classified_setors.csv", row.names = FALSE)

# save to parquet file 
write_parquet(classified_sectors, 
               sink = path_to_output)



