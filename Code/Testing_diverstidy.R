#-----------------------------------PACKAGES-------------------------------

# Installing devtools - a package that helps in installing packages
#from your local directory
install.packages("devtools")
install.packages("arrow")

install.packages("tidyverse")
install.packages("readxl")
install.packages("writexl")
# C:\Users\Saluja_R\OneDrive - Westat\Desktop\Westat\OSS\Sectoring GH\sectoring\diverstidy-main
# using devtools to install tidorgs and diversitdy (make sure to install tidyorgs first as it a dependency for diverstidy) # nolint
devtools::install_local("C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring GH/sectoring/tidyorgs-main/tidyorgs-main", force = TRUE)
devtools::install_local("C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring GH/sectoring/diverstidy-main/diverstidy-main", force = TRUE)

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
library(arrow)



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

# Export to CSV
write.csv(countries_data, "C:\\Users\\Saluja_R\\OneDrive - Westat\\Desktop\\Westat\\OSS\\Sectoring GH\\sectoring\\Code\\output_file.csv", row.names = FALSE)

# Check for all the variables
print(countries_data)
variable_names <- names(countries_data)
print(variable_names)

# --------------CREATING NEW RDA FILE------------------------------

countries <- read.csv('C:\\Users\\Saluja_R\\OneDrive - Westat\\Desktop\\Westat\\OSS\\Sectoring GH\\sectoring\\diverstidy-main\\diverstidy-main\\data-raw\\diverstidy - countries.csv')
save(countries, file = 'C:\\Users\\Saluja_R\\OneDrive - Westat\\Desktop\\Westat\\OSS\\Sectoring GH\\sectoring\\diverstidy-main\\diverstidy-main\\data\\countries_data.rds')


#-------------------------------CHECKING NEW DATA FILE------------------------------

data <- read_parquet('C:\\Users\\Saluja_R\\OneDrive - Westat\\Desktop\\Westat\\OSS\\Sectoring GH\\sectoring\\Code\\user_data_2024_12_04.parquet')


# Re-encode columns that may contain problematic characters
new_data <- data %>%
  mutate(across(everything(), ~ enc2utf8(as.character(.)))) %>%
  mutate(across(everything(), ~ na_if(.x, "NA"))) %>% # Convert "NA" strings to actual NA values
  drop_na() # Remove rows with any NA values


head(new_data)
# Use the detect_geographies function
classified_by_text_new_data <- new_data %>%
  detect_geographies(login, location, "country", author_email)

write.csv(classified_by_text_new_data, file = "C:\\Users\\Saluja_R\\OneDrive - Westat\\Desktop\\Westat\\OSS\\Sectoring GH\\sectoring\\Code\\user_data_output_2024_12_04_2.csv")



library(arrow)

# Specify the path to your Parquet file
parquet_file <- "sectoring\\Code\\user_data_2025_01_06.parquet"
data <- read_parquet(parquet_file)
sv_file <- "sectoring\\Code\\user_data_2025_01_06.csv"

# Write the data to CSV
write.csv(data, sv_file, row.names = FALSE)

nrow(data)
ncol(sv_file)



new_data <- data %>%
  mutate(across(everything(), ~ enc2utf8(as.character(.)))) %>%
  mutate(across(everything(), ~ na_if(.x, "NA"))) %>% # Convert "NA" strings to actual NA values
  drop_na() # Remove rows with any NA values


head(new_data)
# Use the detect_geographies function
classified_by_text_new_data <- new_data %>%
  detect_geographies(login, location, "country", author_email)


# Load the file and check column names
data_path <- "C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring GH/sectoring/diverstidy-main/diverstidy-main/data/countries_data.rda"
load(data_path)

colnames(countries_data)



data <- read.csv("C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring GH/sectoring/Code/user_data_output_2025_1_06_DRAFT1.csv")

colnames(data)
# Total number of values (including NAs)
total_values <- nrow(data)

# Number of NA values
na_values <- sum(is.na(data$country))

# Display results
cat("Total values in 'country' column:", total_values, "\n")
cat("Number of NA values in 'country' column:", na_values, "\n")
cat("Percentage of NA values in 'country' column:", (na_values / total_values) * 100, "%\n")


pipe_values <- sum(grepl("\\|", data$country, fixed = FALSE))

# Display result
cat("Number of values in 'country' column containing '|':", pipe_values, "\n")
cat("Percentage of values in 'country' column containing '|':", (pipe_values / total_values) * 100, "%\n")
cat("Percentage of non NA values in 'country' column containing '|':", (pipe_values / (total_values - na_values)) * 100, "%\n")





# batch_size <- 10000

# # Get total number of rows and batches
# total_rows <- nrow(new_data)
# total_batches <- ceiling(total_rows / batch_size)

# # Initialize progress bar
# pb <- progress_bar$new(
#   format = "Processing Batch :current / :total [:bar] :percent (:elapsed secs)",
#   total = total_batches, clear = FALSE, width = 60
# )

# # Process data in batches
# batch_results <- map_dfr(1:total_batches, function(batch_num) {
#   start_idx <- ((batch_num - 1) * batch_size) + 1
#   end_idx <- min(batch_num * batch_size, total_rows)
  
#   batch_data <- new_data[start_idx:end_idx, ]
  
#   # Run detect_geographies on the batch
#   batch_result <- detect_geographies(batch_data$login, batch_data$location, "country", batch_data$author_email)
  
#   # Update progress bar
#   pb$tick()
  
#   return(batch_result)
# })

# # Combine all batch results into one final dataframe
# classified_by_text_new_data <- batch_results

