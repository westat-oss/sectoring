
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


# detach("package:diverstidy", unload = TRUE)
# detach("package:tidyorgs", unload = TRUE)


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
library(progress)

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

# save(countries_data, file = "C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring GH/sectoring/diverstidy-main/diverstidy-main/data/countries_data.rda")


# For Jeremy: UPDATE PATH TO USER DATA HERE
data <- read_parquet('C:\\Users\\Saluja_R\\OneDrive - Westat\\Desktop\\Westat\\OSS\\Sectoring GH\\sectoring\\Code\\user_data_2025_02_11_filtered.parquet')

#  Re-encode columns that may contain problematic characters   - For Jeremy: THIS WILL TAKE TIME
filtered_data <- data %>%
  mutate(across(everything(), ~ enc2utf8(as.character(.)))) 


# classified_by_countries <- filtered_data %>% 
  # detect_geographies(login, input = c("location", "bio", "socialaccounts"), email = "author_email")
filtered_data <- data
# Define parameters
# colnames(classified_by_countries)

total_rows <- nrow(filtered_data)
num_parts <- 100  # Split data into 100 parts            # For Jeremy: You can change this number to adjust the number of parts
rows_per_part <- ceiling(total_rows / num_parts)  # Rows in each part

# Define base output directory      # For Jeremy: UPDATE THE OUTPUT DIRECTORY HERE
output_base_dir <- "C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring GH/sectoring/Code/Partitioned_Output_2_26"

# Create output directory if it doesn't exist
if (!dir.exists(output_base_dir)) {
  dir.create(output_base_dir, recursive = TRUE)
}

# Set the part number from which to resume processing
resume_from_part <- 1  # For Jeremy: Change this to the part you want to resume from - if running for the first time set to 1

# progress bar
pb <- progress_bar$new(
  format = "Processing Part :current / :total [:bar] :percent (:elapsed secs)",
  total = num_parts - resume_from_part + 1, clear = FALSE, width = 60
)

for (part_num in resume_from_part:num_parts) {
  start_idx <- ((part_num - 1) * rows_per_part) + 1
  end_idx <- min(part_num * rows_per_part, total_rows)
  
  part_data <- filtered_data[start_idx:end_idx, ]

  # Define output folder for this part
  part_output_dir <- file.path(output_base_dir, paste0("Part_", part_num))
  
  # Define output file path
  output_file <- file.path(part_output_dir, paste0("user_data_output_Part_", part_num, ".parquet"))

  # Check if this part has already been processed (file exists)
  if (file.exists(output_file)) {
    cat(sprintf("\nSkipping Part %d (already processed: %s)\n", part_num, output_file))
    pb$tick()  # Progress bar updates even when skipping
    next  # Skip to the next iteration
  }

  # Print which part is running
  cat(sprintf("\nProcessing Part %d out of %d (Rows: %d to %d)\n", part_num, num_parts, start_idx, end_idx))
  
  # Run detect_geographies on this part
  part_result <- part_data %>%
      detect_geographies(
        login, 
        input = c("location", "bio", "socialaccounts"),  
        email = "author_email"
      )
  
  # Create folder for the part if it doesn't exist
  if (!dir.exists(part_output_dir)) {
    dir.create(part_output_dir, recursive = TRUE)
  }

  # Save the output file in its respective folder
  write_parquet(part_result, sink = output_file)
  
  cat(sprintf("\nSaved Part %d output to %s\n", part_num, output_file))

  # Update progress bar
  pb$tick()
}

cat("\nAll remaining parts processed and saved successfully!\n")

# Combine all batch results into one final dataframe

# Define the base directory where partitioned files are stored --For Jeremy: CHANGE THE OUTPUT DIRECTORY ABOVE
output_base_dir <- "C:/Users/Saluja_R/OneDrive - Westat/Desktop/Westat/OSS/Sectoring GH/sectoring/Code/Partitioned_Output"

# List all Parquet files from all parts
parquet_files <- list.files(output_base_dir, pattern = "\\.parquet$", recursive = TRUE, full.names = TRUE)

# Check if there are files to process
if (length(parquet_files) == 0) {
  stop("No Parquet files found in the specified directory.")
}

# Read and combine all Parquet files into a single dataframe
combined_data <- bind_rows(lapply(parquet_files, read_parquet))

# Define the output file path for the merged file
merged_output_file <- file.path(output_base_dir, "user_data_combined.parquet")

# Save the merged data
write_parquet(combined_data, sink = merged_output_file)

cat(sprintf("\nMerged %d files into %s\n", length(parquet_files), merged_output_file))

