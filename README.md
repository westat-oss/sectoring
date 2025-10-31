# Overview

This code repository is used to match GitHub users to countries and industrial sectors. See [Shrivastava and Korkmaz (2024)](https://doi.org/10.6339/24-JDS1148) for an overview of the methodology.

# Repository Structure

Below are the key folders and files in this repo.

- `Code`: Contains R scripts to run organizational sectoring and geographic attribution of GitHub users.

- `Data`: Folder used to store input and output datasets. The R scripts refer extensively to the `Data` folder. The datasets themselves are not tracked with Git as they are large and are stored as binary files (mainly `.parquet`).

- `renv`: A folder used by the `renv` R package to manage R package dependencies.

The `renv.lock` file records the version of R and specific R package versions used for this repo.

# Dependencies

This repository relies heavily on the R packages 'tidyorgs' and 'diverstidy'. We use forked versions of these R packages, [`westat-oss/tidyorgs`](https://github.com/westat-oss/tidyorgs) and [`westat-oss/diverstidy`](https://github.com/westat-oss/diverstidy). Compared to the original versions of those packages, the forked versions incorporate additional changes made by Westat to fix minor issues and to incorporate additional data sources.

We use [the 'renv' R package](https://rstudio.github.io/renv/) to manage R package versions for this repo. The `renv.lock` file specifies the version of R and the versions of specific packages to use for running the code in this repo. By launching the version of R specified in the `renv.lock` file with this repo's base folder as the working directory, you can install the necessary R packages at the specified versions by calling `renv::restore()`.

# Steps to Run the Code

1. Download the Parquet file of GitHub user data: `user_data_2025_02_11.parquet`. Save this file into the 'Data' folder.

2. Launch R in this repo's base folder, so that `renv` will be used and the relative filepaths will work correctly. 

3. Run `renv::restore()` to ensure that necessary dependencies are installed at the required versions.

4. Run the script '01_sectoring_with_tidyorgs.R'. This script reads in user data and conducts sectoring assignments using the 'tidyorgs' R package.

5. Run the script '02_country_assignments_with_diverstidy.R'. This script reads in user data and conducts country assignments using the 'diverstidy' R package.

6. Run the script '03_merge_country_and_sector_data.R'. This merges the outputs from the previous scripts into a single parquet file. The output has the same structure as the original user data supplied to the two previous R scripts, but with additional columns added.

The R script "04_exploratory_data_analysis.R" can be used to conduct exploratory data analysis on the parquet output file created at the end of step 6.

# Notes for Future Development

Running the code in future years will likely require an iterative process of running the 'diverstidy' and 'tidyorgs' functions, checking output, and updating the 'diverstidy' and 'tidyorgs' packages to update their data. Updates to those packages may be necessary to keep up with changes to organization or geographic names that occur over time.