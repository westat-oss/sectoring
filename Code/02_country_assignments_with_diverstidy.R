# Specify display options, load necessary packages ----

options(width = 1000)

library(devtools)
library(tidyorgs)
library(diverstidy)
library(tibble)
library(dplyr)
library(rlang)
library(stringr)
library(tidyverse)
library(readxl)
library(writexl)
library(arrow)
library(progress)

# Specify relevant paths for data
path_to_user_data          <- file.path("Data", "user_data_diff_sample_2025_10_08.parquet")
path_to_partitioned_output <- file.path("Data", "Partitioned_Output_2025_10_08")

# Load input data
data <- read_parquet(path_to_user_data)

#  Re-encode columns that may contain problematic characters   
filtered_data <- data %>%
  mutate(across(everything(), ~ enc2utf8(as.character(.)))) 

# Determine size of chunks to use for data processing
total_rows <- nrow(filtered_data)
num_parts <- 100  # Split data into 100 parts           
rows_per_part <- ceiling(total_rows / num_parts)  # Rows in each part

# Define base output directory      
output_base_dir <- path_to_partitioned_output

# Create output directory if it doesn't exist
if (!dir.exists(output_base_dir)) {
  dir.create(output_base_dir, recursive = TRUE)
}

# Define function to suplement country assignments from diverstidy for specific reasons

add_more_country_assignments <- function(df) {

  new_assignments <- df |> 
    # Drop cases that are already assigned as United States
    group_by(login) |>
    filter(all(is.na(country_location) | country_location != "United States")) |>
    ungroup()

  if (nrow(new_assignments) > 0) { 
    new_assignments <- new_assignments |> mutate(
      lower_location = tolower(location),
      country_location = case_when(
        is.na(lower_location) ~ NA_character_,
        str_detect(lower_location, "new jersey")                      ~ "United States", # Avoid misclassification of New Jersey as the island nation of Jersey 
        str_detect(lower_location, "new mexico")                      ~ "United States", # Avoid misclassification of New Mexico as Mexico
        str_detect(lower_location, "(^|\\b)usa($|\\b)")               ~ "United States", # Check for the word 'usa'
        str_detect(lower_location, "(^|\\b)(nyc|sfo|hou|atl)($|\\b)") ~ "United States", # Check for common, distinctive US city abbreviations used as a word
        TRUE ~ NA_character_
      )
    ) |>
    filter(!is.na(country_location)) |>
    select(-lower_location)
  }
  result <- df |> bind_rows(new_assignments)
  return(result)
}

# Set the part number from which to resume processing
resume_from_part <- 1  # Change this to the part you want to resume from - if running for the first time set to 1

# initialize progress bar
pb <- progress_bar$new(
  format = "Processing Part :current / :total [:bar] :percent (:elapsed secs)",
  total = num_parts - resume_from_part + 1, clear = FALSE, width = 60
)

# iterate over each part
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
  
  # Add country assignments on the current chunk of data
  part_result <- part_data %>%
    detect_geographies(
      login, 
      input = c("location", "bio", "socialaccounts"),  
      email = "author_email"
    ) |>
    add_more_country_assignments()
  
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
