#-----------------------------------PACKAGES-------------------------------

# Installing devtools - a package that helps in installing packages
#from your local directory
install.packages("devtools")

# using devtools to install tidorgs and diversitdy (make sure to install tidyorgs first as it a dependency for diverstidy) # nolint
devtools::install_local("C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring/tidyorgs-main/tidyorgs-main", force = TRUE)
devtools::install_local("C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring/diverstidy-main/diverstidy-main", force = TRUE)

# using the locally installed package
library(tidyorgs)
library(diverstidy)
library(tibble)
library(dplyr)
library(rlang)
library(tidyverse)
library(readxl)
library(writexl)
options(width = 1000)



#----------------------------TESTING ON OLD DATA-----------------------------


old_data <- read.csv("C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring/02_github_sectored_101321.csv",
                     encoding = "UTF-8")

# Re-encode columns that may contain problematic characters
old_data <- old_data %>%
  mutate(across(everything(), ~ enc2utf8(as.character(.)))) %>%
  mutate(across(everything(), ~ na_if(.x, "NA"))) %>% # Convert "NA" strings to actual NA values
  drop_na() # Remove rows with any NA values

# Use the detect_geographies function
classified_by_text <- old_data %>%
  detect_geographies(login, location, "country", email)

# Print the result
head(classified_by_text)

write_xlsx(classified_by_text, "C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring/02_github_sectored_101321_output.xlsx")

#checking column names in both the files
colnames(old_data)
colnames(classified_by_text)

# Count rows where 'country_original' and 'country' are different
different_rows <- classified_by_text %>%
  filter(country_original != country)

# Number of rows with different values
num_different_rows <- nrow(different_rows)

# Output the result
cat("Number of rows with different values:", num_different_rows)

# Find the row where 'country_original' and 'country' differ
different_row <- classified_by_text %>%
  filter(country_original != country)

# Print the values
print(different_row)

#-----------------------------------CHECKING .RDA FILES-------------------------

# The package calls a .rda file. Review the file.
load("C:\\Users\\Saluja_R\\OneDrive - Westat\\Desktop\\Westat\\OSS\\Sectoring\\diverstidy-main\\diverstidy-main\\data\\countries_data.rda")  

# Check for all the variables
print(countries_data)
variable_names <- names(countries_data)
print(variable_names)
