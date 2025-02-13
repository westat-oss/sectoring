# Change the path 

install.packages("devtools")
install.packages("arrow")
install.packages("rlang")
install.packages("stringr")
install.packages("tibble")
install.packages("tidyverse")
install.packages("readxl")
install.packages("writexl")
install.packages("dplyr")

# detach the packages before installing so we get the updated packages

detach("package:tidyorgs", unload = TRUE)

devtools::install_local("C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring GH/sectoring/tidyorgs-main/tidyorgs-main", force = TRUE)

detach("package:diverstidy", unload = TRUE)

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

# UPDATES - INSIDE THE PACKAGE

data <- read_parquet('C:\\Users\\Saluja_R\\OneDrive - Westat\\Desktop\\Westat\\OSS\\Sectoring GH\\sectoring\\Code\\user_data_2024_12_04.parquet')

classified_sectors <- data %>%
  detect_academic(login, company, organization_academic, country = TRUE, email = author_email)

classified_sectors <- classified_sectors %>%
    rename(country_academic = country)

classified_sectors <- classified_sectors %>%
  detect_business(login, company, organization_business, country = TRUE, email = author_email)

classified_sectors <- classified_sectors %>%
    rename(country_business = country)

classified_sectors <- classified_sectors %>%
  detect_government(login, company, organization_government, country = TRUE, email = author_email)


classified_sectors <- classified_sectors %>%
    rename(country_government = country)

classified_sectors <- classified_sectors %>%
  detect_nonprofit(login, company, organization_nonprofit, country = TRUE, email = author_email)

classified_sectors <- classified_sectors %>%
    rename(country_nonprofit = country)

# to create a parquest file
write_parquet(classified_sectors, 
               sink = "C:\\Users\\Saluja_R\\OneDrive - Westat\\Desktop\\Westat\\OSS\\Sectoring GH\\sectoring\\Code\\user_data_output_2024_12_04_secotorsParquet.parquet")


