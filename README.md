# Overview

This code repository is used to match GitHub users to countries and industrial sectors. See [Shrivastava and Korkmaz (2024)](https://doi.org/10.6339/24-JDS1148) for an overview of the methodology.

# Steps to Run the Code

1. Download the Parquet file of GitHub user data: `user_data_2025_02_11.parquet`. Save this file into the 'Data' folder.

2. Update the file paths in 'Updates_tidyorgs.R' and 'Updates_diverstidy.R'

    - path_to_tidyorgs
    - paths_to_diverstidy
    - path_to_user_data
    - path_to_partitioned_output

3. Run 'Updates_tidyorgs.R'

4. Run 'Updates_diverstidy.R' 

5. To merge the two outputs, run 'EDA_NewCountry.R' 

    - NOTE: This file is not yet saved on GitHub- Rashi is resolving issues with this script before adding it to the Git repo.