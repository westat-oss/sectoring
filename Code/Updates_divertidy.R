
# skip if you have already installed the packages in updates_tidyorgs.R
install.packages("devtools")
install.packages("arrow")
install.packages("rlang")
install.packages("stringr")
install.packages("tibble")
install.packages("tidyverse")
install.packages("readxl")
install.packages("writexl")
install.packages("dplyr")

# Update the package path 
# make sure to install tidyorgs first as it a dependency for diverstidy

detach("package:diverstidy", unload = TRUE)
detach("package:tidyorgs", unload = TRUE)
devtools::install_local("C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring GH/sectoring/tidyorgs-main/tidyorgs-main", force = TRUE)
devtools::install_local("C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring GH/sectoring/diverstidy-main/diverstidy-main", force = TRUE)


library(tidyorgs)
library(diverstidy)
library(tibble)
library(dplyr)
library(rlang)
library(stringr)
library(tidyverse)
library(readxl)
library(writexl)
options(width = 1000)
library(arrow)

# ------------------LOADING THE DATA------------------

# Load the file and check column names
data_path <- "C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring GH/sectoring/diverstidy-main/diverstidy-main/data/countries_data.rda"
load(data_path)

# ------------------CHANGES TO THE DATA------------------

# Define updates: List of countries and the cities to add

updates <- list(
  list(country = "italy|italia", cities_to_add = "|bastia umbra"),
  list(country = "indonesia", cities_to_add = "|indonesian"),
  list(country = "united states of america|united states|usa", cities_to_add = "|bayonne|odessa tx|odessa|untied states"),
  list(country = "india", cities_to_add = "|goa|cochin|chengannur"),
  list(country = "bulgaria", cities_to_add = "|madara"),
  list(country = "united kingdom|great britain|uk|gb|england|scotland|wales|northern ireland|great britain|britain", cities_to_add = "|great bri ain"),
  list(country = "south korea|korea republic of|korea republic of|rep of korea|republic of korea|korea republic", cities_to_add = "|korea"),
  list(country = "france", cities_to_add = "|sophia antipolis")
)

# Iterate and update the existing data
for (update in updates) {
  row_index <- which(countries_data$countries == update$country)
  
  if (length(row_index) > 0) {  # Ensure the country exists in the dataset
    countries_data$cities[row_index] <- paste0(countries_data$cities[row_index], update$cities_to_add)
    countries_data$recode_cities[row_index] <- paste0(countries_data$recode_cities[row_index], update$cities_to_add)
  }
}


# ------------------REMOVING 'NEW' FROM 'JERSEY' COUNTRY------------------
row_index <- which(countries_data$countries == "jersey")
if (length(row_index) > 0) {  # Ensure the country exists in the dataset
  # Remove "(?!new )" from recode_countries
  countries_data$recode_countries[row_index] <- gsub("\\(\\?!new \\)", "", countries_data$recode_countries[row_index])
}

# update the path
save(countries_data, file = "C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring GH/sectoring/diverstidy-main/diverstidy-main/data/countries_data.rda")


#------------------TESTING THE DATA------------------

# update the path to users data here
data <- read_parquet('C:\\Users\\Saluja_R\\OneDrive - Westat\\Desktop\\Westat\\OSS\\Sectoring GH\\sectoring\\Code\\user_data_2024_12_04.parquet')

# Re-encode columns that may contain problematic characters
filtered_data <- data %>%
  mutate(across(everything(), ~ enc2utf8(as.character(.)))) 



# detach the package and install again to make sure the updates are applied
detach("package:diverstidy", unload = TRUE)

# change path here
devtools::install_local("C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring GH/sectoring/diverstidy-main/diverstidy-main", force = TRUE)
# load the package again
library(diverstidy)

classified_by_countries <- filtered_data %>% 
  detect_geographies(login, input = c("location", "bio", "socialaccounts"), output = "country", email = author_email)


# CLEANING STEPS (once the output is formed)

# PROCESS 1: To see if the is a state abbreviation in the location and the country is United States
state_codes <- c("al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky", 
                 "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", 
                 "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", 
                 "wv", "wi", "wy", "nyc", "dc", "sf")

classified_by_countries <- classified_by_countries %>%
  mutate(location = tolower(location), # Ensure lowercase for comparison
         country = case_when(
           !is.na(location) & location != "nothing" &  # location should not be null or nothing
           substr(location, nchar(location)-1, nchar(location)) %in% state_codes &  # checking if the last two characters correspond to a state abbreviation
           str_detect(tolower(country), fixed("united states", ignore_case = TRUE)) ~ "United States", # checking if the country is United States - detected already by the package
           TRUE ~ country
         ))

# PROCESS 2: Remove all NAs from the country column
classified_by_countries <- classified_by_countries %>%
  mutate(
    # Step 1: If "NA" is present and only one "|", remove both "NA" and "|"
    country = if_else(
      str_count(country, "\\|") == 1 & str_detect(country, "NA"),
      str_replace_all(country, "\\|?NA\\|?", ""),  # Remove single "NA" and "|"
      country
    )
  ) %>%
  mutate(
    # Step 2: If multiple "|" exist, remove "|NA" and "NA|"
    country = if_else(
      str_count(country, "\\|") > 1,
      str_replace_all(str_replace_all(country, "\\|NA", ""), "NA\\|", ""),
      country
    )
  ) %>%
  mutate(
    # Step 3: Remove any remaining standalone "|" if present
    country = str_replace_all(country, "^\\||\\|$", "")
  )


# PROCESS 3: If the location is New Jersey then it it United States and not Jersey
classified_by_countries <- classified_by_countries %>%
    mutate(
            country = if_else(
            str_detect(tolower(location), "new jersey"),
            "United States",
            country
            )
    )



# save the file
# write.csv(classified_by_countries, 
#           file = "C:\\Users\\Saluja_R\\OneDrive - Westat\\Desktop\\Westat\\OSS\\Sectoring GH\\sectoring\\Code\\user_data_output_2024_12_04_DRAFT12.csv")

# to create a parquest file
write_parquet(classified_by_countries, 
               sink = "C:\\Users\\Saluja_R\\OneDrive - Westat\\Desktop\\Westat\\OSS\\Sectoring GH\\sectoring\\Code\\user_data_output_2024_12_04_DRAFT12.parquet")

