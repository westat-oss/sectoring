#' Convert messy text and email data into standardized geographies such as countries, country codes, and continents.
#'
#' This function standardizes messy text data that contains city, region, and/or country names 
#' as well as email domains into standardized geographic entities. The detect_geographies() function 
#' relies on a "funnel matching" method that unnests text and then reiterates over n-grams, matching 
#' all words sequences from n to n = 1 without much use of regular expressions or text cleaning. 
#' Currently, the function offers 14 output types including countries, continents, flag emojis, and seven languages. 
#'
#' @param data A data frame or data frame extension (e.g. a tibble).
#' @param id A numeric or character vector unique to each entry.
#' @param input Character vector of text data that includes the name of cities, states, 
#' and/or countries that will be standardized into country names or country codes. If multiple countries are detected, they will be separated by the "|" symbol.
#' @param output Output column. Options include 'country', 'iso2', 'iso3', 'flag', 'continent', 'region', 'sub_region', 'int_region', 'country_arabic', 'country_chinese', 'country_french', 'country_russian', and 'country_spanish'.
#' @param email Character vector of email or email domain information. Defaults to FALSE
#' @param cities Optional argument to detect major cities in each country. Defaults to TRUE.
#' @param denonyms Optional argument to detect denonyms of inhabitants of each country. Defaults to TRUE.
#'
#' @examples
#'
#' library(tidyverse)
#' library(diverstidy)
#' data(github_users)
#'
#' classified_by_text <- github_users %>%
#'   detect_geographies(login, location, "country", email)
#'
#' @export
detect_geographies <- function(data, id, input, 
                               output = c("country", "iso_2", "iso_3", "flag", 
                                          "continent", "region", "sub_region", 
                                          "int_region", "country_chinese", "country_russian", 
                                          "country_french", "country_spanish", "country_arabic"), 
                              email = FALSE, cities = TRUE, denonyms = TRUE ){
  print('Check: Custom Package')
  pb <- progress::progress_bar$new(total = 100)
  pb$tick(0)
  # 2. convert all vars with enquos
  id <- dplyr::enquo(id)
  # --------------------------------------------UPDATED-----------------------------------------------

  # input <- dplyr::enquo(input)
  output <- rlang::arg_match(output)
  `%notin%` <- base::Negate(`%in%`)

  # Ensure input is a character vector of column names
  if (!is.character(input)) {
    stop("Error: 'input' must be a character vector of column names.")
  }

  # --------------------------------------------UPDATED-----------------------------------------------
  # print("Debugging Output:")
  # print(paste("id: ", as.character(id)))           # Convert quosure to character for display
  # print(paste("input: ", as.character(input)))     # Convert quosure to character for display
  # print(paste("output: ", output))      

  # 1. prep the dictionary procedure 
  # 1a. pull in countries dictionary 
  # dictionary <- diverstidy:::countries_data

  # dictionary <- get("countries_data", envir = asNamespace("diverstidy"), inherits = TRUE)
  dictionary <- as.data.frame(get("countries_data", envir = asNamespace("diverstidy"), inherits = TRUE))


  



  # --------------------------------------------UPDATED-----------------------------------------------
  # if (is.null(dictionary)) {
  #   stop("Error: countries_data is NULL. Check if the diverstidy package is loaded correctly.")
  # } else {
  #   print("Successfully accessed countries_data:")
  # }

  # print(head(dictionary, 5))

  # 1b. create all the conditions to detect countries, regions and cities 
  dictionary <- dictionary %>% 
    dplyr::rename(catch_terms = countries, recode_column = recode_countries)
  
  if (missing(id)) { 
    return(print("Error: 'id' column requires numeric or character vector."))
  } else if (missing(input)) { 
    return(print("Error: 'input' column requires character vector."))
  #} else if (regions == TRUE) {dictionary <- dictionary %>% 
  #  tidyr::unite(catch_terms, c("catch_terms", "regions"), sep="|") %>% 
  #  tidyr::unite(recode_column, c("recode_column", "recode_regions"), sep="|") %>% 
  #  dplyr::mutate(catch_terms = stringr::str_replace_all(catch_terms, "\\|NULL", "")) %>% 
  #  dplyr::mutate(recode_column = stringr::str_replace_all(recode_column, "\\|NULL", ""))
  } else if (cities == TRUE) {dictionary <- dictionary %>% 
    tidyr::unite(catch_terms, c("catch_terms", "cities"), sep="|") %>% 
    tidyr::unite(recode_column, c("recode_column", "recode_cities"), sep="|") %>% 
    dplyr::mutate(catch_terms = stringr::str_replace_all(catch_terms, "\\|NULL", "")) %>% 
    dplyr::mutate(recode_column = stringr::str_replace_all(recode_column, "\\|NULL", ""))
  } else if (denonyms == TRUE) {dictionary <- dictionary %>% 
    tidyr::unite(catch_terms, c("catch_terms", "denonyms"), sep="|") %>% 
    tidyr::unite(recode_column, c("recode_column", "recode_denonyms"), sep="|") %>% 
    dplyr::mutate(catch_terms = stringr::str_replace_all(catch_terms, "\\|NULL", "")) %>% 
    dplyr::mutate(recode_column = stringr::str_replace_all(recode_column, "\\|NULL", ""))
  #} else if (abbreviations == TRUE) {dictionary <- dictionary %>% 
  #  tidyr::unite(catch_terms, c("catch_terms", "abbreviations"), sep="|") %>% 
  #  tidyr::unite(recode_column, c("recode_column", "recode_abbreviations"), sep="|") %>% 
  #  dplyr::mutate(catch_terms = stringr::str_replace_all(catch_terms, "\\|NULL", "")) %>% 
  #  dplyr::mutate(recode_column = stringr::str_replace_all(recode_column, "\\|NULL", ""))
  } else { dictionary }
  
  
  # TESTING
  # List of missing locations
  # missing_locations <- c("Cochin", "Chengannur", "Sophia-Antipolis")

  # # Check if these locations exist in catch_terms
  # matching_terms <- dictionary %>%
  #   filter(str_detect(catch_terms, paste(missing_locations, collapse = "|")))

  # # Display matching dictionary entries
  # print(matching_terms)


  # print(names(dictionary))  # Returns the keys in the dictionary
  # print(head(dictionary,2))



  # 1d. prior to running the for loop, we need the max string length 
  max_n <- dictionary %>%
    tidyr::unnest_legacy(catch_terms = base::strsplit(catch_terms, "\\|")) %>%
    dplyr::mutate(word_count = lengths(base::strsplit(catch_terms, "\\W+"))) 
  max_n <- max(max_n$word_count) 
  
  # 3. drop missing, convert to lower case, convert foreign characters to english
  suppressMessages(string_corrections <- diverstidy::string_corrections)
  string_corrections <- string_corrections %>% 
    dplyr::mutate(recode_column = paste0(" ",recode_column," ")) %>%
    dplyr::select(original_string, recode_column) %>% tibble::deframe()

  # need to rename column if the original df has "country" as a column name 
  if (output == "country" && "country" %in% colnames(data)) {
    data <- plyr::rename(data, replace = c(country="country_original"), warn_missing = FALSE)
      warning("The original data frame contained the same name as your 'output' variable. The column will be renamed.")
  } else if (output == "continent" && "continent" %in% colnames(data)) {
    data <- plyr::rename(data, replace = c(continent="continent_original"), warn_missing = FALSE)
    warning("The original data frame contained the same name as your 'output' variable. The column will be renamed.")
  } else { data } # need to expand the rest of this as well  
  
  # --------------------------UPDATED--------------------------
  # Combine the specified input columns into a single text column for processing
  data <- data %>% 
    dplyr::mutate(combined_input = dplyr::across(all_of(input), ~ ifelse(is.na(.), "", .)) %>% 
                    purrr::pmap_chr(paste, collapse = " "))
  # --------------------------UPDATED CODE ENDS HERE-------------
  
  original_data <- data

  #-------------------------OLD CODE-------------------------
  # data <- data %>% 
  #   tidyr::drop_na(!!input) %>%
  #   dplyr::mutate("{{input}}" := tolower(!!input),
  #                 "{{input}}" := stringr::str_replace_all(!!input, "/", " "),
  #                 "{{input}}" := stringr::str_replace_all(!!input, "\\.", " "),
  #                 "{{input}}" := stringr::str_replace_all(!!input, "·", " "),
  #                 "{{input}}" := stringr::str_replace_all(!!input, "_", " "),
  #                 "{{input}}" := stringr::str_replace_all(!!input, "\\b(:)\\b", " "),
  #                 "{{input}}" := stringr::str_replace_all(!!input, "u\\.s\\.","united states"),
  #                 "{{input}}" := stringr::str_replace_all(!!input, "u\\.s\\.a\\.","united states"),
  #                 "{{input}}" := stringr::str_replace_all(location, string_corrections),
  #                 "{{input}}" := stringr::str_replace_all(!!input, ",", " "))

# ---------------------OLD CODE ENDS HERE---------------------

# ---------------------NEW CODE STARTS HERE---------------------
  data <- data %>%
  tidyr::drop_na(combined_input) %>%
  dplyr::mutate(combined_input = tolower(combined_input),
                combined_input = stringr::str_replace_all(combined_input, "/", " "),
                combined_input = stringr::str_replace_all(combined_input, "\\.", " "),
                combined_input = stringr::str_replace_all(combined_input, "·", " "),
                combined_input = stringr::str_replace_all(combined_input, "_", " "),
                combined_input = stringr::str_replace_all(combined_input, "\\b(:)\\b", " "),
                combined_input = stringr::str_replace_all(combined_input, "u\\.s\\.","united states"),
                combined_input = stringr::str_replace_all(combined_input, "u\\.s\\.a\\.","united states"),
                combined_input = stringr::str_replace_all(combined_input, ",", " "))

#---------------------NEW CODE ENDS HERE---------------------

  pb$tick(5)
  # 4. use a for loop to funnel match n-grams of lengths 2-12 
  funnelized <- data.frame()
  for (n_word in max_n:2) {
    # note: 6 is the longest string in the location data (as of 08-25-2021)
    subdictionary <- dictionary %>%
      tidyr::unnest_legacy(catch_terms = base::strsplit(catch_terms, "\\|")) %>%
      dplyr::mutate(word_count = lengths(base::strsplit(catch_terms, "\\W+"))) %>%
      dplyr::filter(word_count == n_word)
    subdictionary <- stats::na.omit(subdictionary$catch_terms)
    funnelized <- data %>%
      tidytext::unnest_tokens(words, combined_input, token="ngrams", n=n_word, to_lower = TRUE) %>%  #CHANGE MADE HERE
      dplyr::filter(words %in% subdictionary) %>%
      dplyr::select(!!id, words) %>%
      dplyr::bind_rows(funnelized)
  }

  # TESTING
  # write.csv(funnelized, "funnelized_step1_debug.csv", row.names = FALSE)

  # 5. funnel match on all of the single tokens 
  subdictionary <- dictionary %>%
    tidyr::unnest_legacy(catch_terms = base::strsplit(catch_terms, "\\|")) %>%
    dplyr::mutate(word_count = lengths(base::strsplit(catch_terms, "\\W+"))) %>%
    dplyr::filter(word_count == 1)
  subdictionary <- stats::na.omit(subdictionary$catch_terms)
  funnelized <- data %>%
    #dplyr::filter(!!id %notin% ids_to_filter) %>%
    tidytext::unnest_tokens(words, combined_input) %>%            # CHANGE MADE HERE
    dplyr::filter(words %in% subdictionary) %>%
    dplyr::select(!!id, words) %>%
    dplyr::bind_rows(funnelized) %>% 
    dplyr::select(!!id, words) 
  dictionary <- dictionary %>%
    dplyr::mutate(original_string = paste0("\\b(?i)(",recode_column,")\\b")) %>%
    dplyr::select(original_string, !!output) %>% tibble::deframe()
  pb$tick(15)
  
  # TESTING
  # write.csv(funnelized, "funnelized_step2_debug.csv", row.names = FALSE)
  # TESTING
  # print(str(dictionary))
  # print(str(funnelized))
  # print(class(funnelized$words))
  # print(class(funnelized$id))
  # write.csv(funnelized, "funnelized_debug.csv", row.names = FALSE)
  # print("Debug: Checking Geo Code Mapping for Bayonne")
  # print(funnelized %>% filter(words == "bayonne") %>% mutate(mapped_code = stringr::str_replace_all(words, dictionary)))
  # print(names(dictionary))
  
  
  # ORIGINAL CODE
  # all_matched_data <- funnelized %>%
  #   dplyr::mutate(geo_code = stringr::str_replace_all(words, dictionary)) %>% 
  #   dplyr::select(!!id, geo_code) 


  # UPDATED CODE STARTS HERE
  get_geo_code <- function(word, dictionary) {
    regex_patterns <- tolower(names(dictionary))
    regex_patterns <- regex_patterns[!is.na(regex_patterns)]
    matched_indices <- str_detect(word, regex_patterns)
    matches <- dictionary[matched_indices]
    if (length(matches) > 0) {
      return(matches)
    } else {
      return(NA_character_)  # Ensure NA is treated as character
    }
  }

  # Ensure 'login' and 'words' columns exist
  if (!"login" %in% colnames(funnelized) | !"words" %in% colnames(funnelized)) {
    stop("Columns 'login' or 'words' not found in funnelized")
  }

  # Create all_matched_data dataframe without 'words' column
  all_matched_data <- funnelized %>%
    mutate(geo_code = lapply(words, get_geo_code, dictionary = dictionary)) %>%
    unnest(geo_code) %>%
    select(login, geo_code)  # Keeping only login and geo_code

  # UPDATED CODE ENDS HERE


  
  
  # TESTING
  # write.csv(all_matched_data, "all_matched_data_debug_3.csv", row.names = FALSE)
  # print("Debug: Checking Initial Matches for Bayonne Before Aggregation")
  # print(all_matched_data %>% filter(words == "bayonne"))
  
  pb$tick(60)
  # going to use this vector to filter later on 
  geography_list <- c(stats::na.omit(countries_data$country), 
                      stats::na.omit(countries_data$iso_2),
                      stats::na.omit(countries_data$iso_3), 
                      stats::na.omit(countries_data$continent),
                      stats::na.omit(countries_data$flag), 
                      stats::na.omit(countries_data$region),
                      stats::na.omit(countries_data$sub_region), 
                      stats::na.omit(countries_data$int_region),
                      stats::na.omit(countries_data$country_arabic), 
                      stats::na.omit(countries_data$country_chinese), 
                      stats::na.omit(countries_data$country_french), 
                      stats::na.omit(countries_data$country_russian),
                      stats::na.omit(countries_data$country_spanish))
  
  
  
  # aggregate all matched data 
  all_matched_data <- all_matched_data %>% 
    dplyr::distinct(across(everything())) %>% 
    dplyr::filter((is.na(geo_code) | geo_code %in% geography_list) & geo_code != "NA") %>% # added here 
    dplyr::group_by(!!id, geo_code) %>%
    dplyr::mutate(geo_code = paste(geo_code, collapse = "|")) %>% 
    dplyr::distinct(across(everything())) %>%
    dplyr::mutate(geo_code = dplyr::na_if(geo_code, "NA")) %>% 
    dplyr::rename("{{output}}" := geo_code) %>% 
    dplyr::rename_all(~stringr::str_replace_all(.,"\"",""))
  pb$tick(10)

  # TESTING
  # write.csv(all_matched_data, "all_matched_data_debug.csv", row.names = FALSE)

 
  # put it all together 
  if (missing(email)) { 
    # if no emails then do this 
    suppressMessages(
      data <- original_data %>% 
        dplyr::left_join(all_matched_data) %>% 
        dplyr::rename(geo_code = !!output) %>% 
        dplyr::distinct(across(everything())) %>%
        # removing %notin% reveals regex still to fix
        #dplyr::filter((is.na(geo_code) | geo_code %in% geography_list) & geo_code != "NA") %>% 
        dplyr::group_by(!!id, combined_input) %>%                             #CHANGE MADE HERE
        dplyr::mutate(geo_code =  paste0(geo_code, collapse = "|")) %>% 
        dplyr::distinct(across(everything())) %>%
        dplyr::mutate("{{output}}" := dplyr::na_if(geo_code, "NA")) %>% 
        dplyr::rename_all(~stringr::str_replace_all(.,"\"","")) %>% 
        dplyr::ungroup() %>% 
        dplyr::select(-geo_code))
    } else {
    # 6. otherwise, match by emails 
    email <- enquo(email)
    # 6a. pull out all of the country domains from the dictionary 
    country_dictionary <- diverstidy::countries_data
    country_dictionary <- country_dictionary %>%
      tidyr::drop_na(iso_domain) %>%
      tidyr::unnest_legacy(iso_domain = base::strsplit(iso_domain, "\\|")) %>% 
      dplyr::select(iso_domain, !!output) 
    country_vector <- stats::na.omit(country_dictionary$iso_domain)
    country_dictionary <- country_dictionary %>%
      dplyr::mutate(beginning = "\\b(?i)(", ending = ")\\b", 
                    iso_domain = stringr::str_replace(iso_domain, "\\.", ""),
                    iso_domain = paste0(beginning, iso_domain, ending)) %>%
      dplyr::select(iso_domain, !!output) %>% tibble::deframe()
    # 6b. drop missing emails, all domains to lower, extract domain info after @ sign
    matched_by_email <- original_data %>%
      tidyr::drop_na(!!email) %>% # drop missing emails
      dplyr::mutate("{{ email }}" := tolower(!!email)) %>% # all domains to lower
      dplyr::mutate(domain = sub('.*@', '', !!email)) %>% # extract domain info after @ sign
      dplyr::mutate(domain = sub('.*\\.', '.', domain)) %>% # extract the last .domain
      # matches all of the root domains with several exceptions (bc of industry appropriation)
      dplyr::filter(domain %in% country_vector 
                    & domain != ".ag" & domain != ".ai" & domain != ".am" 
                    & domain != ".as" & domain != ".cc" & domain != ".fm" 
                    & domain != ".io" & domain != ".im" & domain != ".me") %>%  
      dplyr::mutate(domain = str_replace(domain, '\\.', '')) 
    # 6c. uses str_replace_all() to recode all domains into countries
    matched_by_email <- matched_by_email %>% 
      dplyr::mutate(geo_code = stringr::str_replace_all(domain, country_dictionary)) %>%
      dplyr::select(!!id, geo_code)
    # 6d. removes all of the duplicates and combines those with multiple countries
    matched_by_email <- matched_by_email %>% 
      dplyr::distinct(across(everything())) %>%
      dplyr::group_by(!!id, geo_code) %>%
      dplyr::mutate(geo_code = paste(geo_code, collapse = "|")) %>% 
      dplyr::distinct(across(everything())) %>%
      dplyr::mutate("{{output}}" := dplyr::na_if(geo_code, "NA")) %>% 
      dplyr::rename_all(~stringr::str_replace_all(.,"\"","")) %>% 
      dplyr::ungroup() %>% 
      dplyr::select(-geo_code)
    all_matched_data <- dplyr::bind_rows(all_matched_data, matched_by_email) 
    suppressMessages(
      data <- original_data %>% 
        dplyr::left_join(all_matched_data) %>%
        dplyr::rename(geo_code = !!output) %>% 
        dplyr::distinct(across(everything())) %>%
        # removing %notin% reveals regex still to fix
        #dplyr::filter((is.na(geo_code) | geo_code %in% geography_list) & geo_code != "NA") %>% 
        dplyr::group_by(!!id, combined_input, !!email) %>%                         # CHANGE MADE HERE
        dplyr::mutate(geo_code =  paste0(geo_code, collapse = "|")) %>% 
        dplyr::distinct(across(everything())) %>%
        dplyr::mutate("{{output}}" := dplyr::na_if(geo_code, "NA")) %>% 
        dplyr::rename_all(~stringr::str_replace_all(.,"\"","")) %>% 
        dplyr::ungroup() %>% 
        dplyr::select(-geo_code) 
    ) 
  } 
  pb$tick(10)
  

  data
}