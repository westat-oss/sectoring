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

path_to_user_data <- "Data/user_data_2025_10_08.parquet"
path_to_output_temp    <- "Intermediatary Data/user_data_sectors_2025_10_08.parquet"

# Load user data ----

data <- read_parquet(path_to_user_data)
nrow(data)
colnames(data)
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

# Business

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


government_data <- tidyorgs::gov_data

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

write_parquet(classified_sectors, sink = path_to_output_temp)

#
# write code to run the tidyorgs output through diverstidy here


data <- read_parquet(path_to_output_temp)
colnames(data)

filtered_data <- data %>%
  mutate(across(everything(), ~ enc2utf8(as.character(.))))

path_to_partitioned_output <- 'Output/10_08_2025_diversity on sectoring'
total_rows <- nrow(filtered_data)
num_parts <- 100  # Split data into 100 parts           
rows_per_part <- ceiling(total_rows / num_parts)  # Rows in each part

# Define base output directory      
output_base_dir <- path_to_partitioned_output

# Create output directory if it doesn't exist
if (!dir.exists(output_base_dir)) {
  dir.create(output_base_dir, recursive = TRUE)
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
  
  # Run detect_geographies on this part
  part_result <- part_data %>%
      detect_geographies(
        login, 
        input = c("country_academic", "country_business", "country_government", "country_nonprofit")
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


## -------------TEMP EDA and CLEANING -------------------


data_new <- read_parquet('Output/10_08_2025_diversity on sectoring/user_data_combined.parquet')
nrow(data_new)
nrow(data)            #7363285

duplicates <- data_new %>%
  group_by(id) %>%
  filter(n() > 1) %>%
  arrange(id)

# Check how many duplicate rows
nrow(duplicates)

# Save to Excel
write_xlsx(duplicates, "Output/10_08_2025_diversity on sectoring/duplicate_rows.xlsx")


# with_pipe <- data %>%
#   filter(grepl("\\|", country_business))

# # Count how many such rows
# nrow(with_pipe)
data_new <- data_new %>%
  mutate(
    country_academic = if_else(
      organization_company_academic == "Université Laval", 
      "Canada", 
      country_academic
    ),
    country_country_academic = if_else(
      organization_company_academic == "Université Laval", 
      "Canada", 
      country_country_academic
    )
  ) %>%
  distinct()


mismatches <- data_new %>%
  filter(
    # academic mismatches
    (country_academic != country_country_academic) |
    (is.na(country_academic) & !is.na(country_country_academic)) |
    (!is.na(country_academic) & is.na(country_country_academic)) |
    
    # government mismatches
    (country_government != country_country_government) |
    (is.na(country_government) & !is.na(country_country_government)) |
    (!is.na(country_government) & is.na(country_country_government))
  )

nrow(mismatches)
write_xlsx(mismatches, "Output/10_08_2025_diversity on sectoring/mismatches.xlsx")

data_new <- data_new %>%
  mutate(
    country_country_academic = case_when(
      country_academic == "Georgia" ~ "Georgia",
      country_academic == "South Sudan" ~ "South Sudan",
      country_academic == "Germany" ~ "Germany",
      TRUE ~ country_country_academic
    )
  )

data_new <- data_new %>%
  select(-country_academic) %>%                  # drop column
  rename(country_academic = country_country_academic)  # rename column

data_new <- data_new %>%
  select(-country_business) %>%                  # drop column
  rename(country_business = country_country_business)  # rename column

data_new <- data_new %>%
  select(-country_nonprofit) %>%                  # drop column
  rename(country_nonprofit = country_country_nonprofit)  # rename column

data_new <- data_new %>%
  select(-country_government) %>%                  # drop column
  rename(country_government = country_country_government)  # rename column
colnames(data_new)

country_cols <- grep("^country_", names(data_new), value = TRUE)

# define replacement mapping
replacements <- c(
  "Czech Republic" = "Czechia",
  "Hong Kong" = "China"
)

# apply replacements across all country_* columns
data_new <- data_new %>%
  mutate(across(all_of(country_cols),
                ~ dplyr::recode(., !!!replacements)))


# remaining <- data_new %>%
#   summarise(across(all_of(country_cols),
#                    ~ sum(. == "Colombia", na.rm = TRUE)))


write_parquet(data_new, sink = 'Output/user_data_sectors_2025_10_08.parquet')



## TEMP CHECKS

# data_update <- read_parquet('Data/user_data_8_17_2025.parquet')
# nrow(data_update)
# colnames(data_update)
# dupes <- data_update %>%
#   filter(duplicated(id) | duplicated(id, fromLast = TRUE)) %>%
#   arrange(id)

# # Check how many
# nrow(dupes)

# # Save to Excel
# write_xlsx(dupes, "Output/duplicated_logins.xlsx")
# data_update <- data_update %>%
#   mutate(updatedat = ymd_hms(updatedat))   # or ymd() if only dates

# # get latest value
# latest_date <- max(data_update$updatedat, na.rm = TRUE)





# germany_rows <- data_new %>%
#   filter(
#     str_detect(country_academic, "Germany") |
#     str_detect(country_government, "Germany")
#   )

# nrow(germany_rows)

# # write to Excel
# write_xlsx(germany_rows, "Output/germany_academic_government.xlsx")


# part_data <- tibble(
#   login = c("user1"),
#   country_academic = c("Germany"))

# # run detect_geographies
# part_result <- part_data %>%
#   detect_geographies(
#     login,
#     input = c("country_academic")
#   )
# part_result
