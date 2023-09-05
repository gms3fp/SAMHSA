# Info --------------------------------------------------------------------

# Galen Sheely
# University of Virginia
# SAMHSA Total SMHA Expenditures Scraping
# Kirkland RA Fall 2023

# Library -----------------------------------------------------------------

library(rvest)
library(tidyverse)
library(glue)
library(purrr)
library(janitor)
library(pdftools)
library(beepr)
library(labelled)
library(magrittr)
library(furrr)
library(tictoc)
library(renv)
here::i_am("code/1_scrape.R")
library(here)

# Settings / Convenience Functions ----------------------------------------

# Just read_html wrapped with possibly to keep it from bricking long loops
safe_read_html <- possibly(read_html, 
                           otherwise = "Empty Page")

# Grabs hrefs as list, returns "Error" as a list if there is an error
grab_href <- possibly(
  function(target, element){
    list(target %>% 
           html_elements(element) %>% 
           html_attr("href"))},
  otherwise = list("Error"))

# Setting parallel options
# This may need to be adjusted according to CPU of user
plan(multisession, workers = 12)


# States/Territories to Iterate Over --------------------------------------

states <- readRDS(here("data", "states.Rds")) %>% 
  select(state) %>% # Getting state strings ready for scraping SAMHSA
  mutate(st_str = str_to_lower(state),
         st_str = str_replace_all(st_str, " ", "-"),
         st_str = case_when(
           st_str == "district-of-columbia" ~ "dc",
           TRUE ~ st_str 
         ))

# Territories in SAMHSA Data (Might as well)
territories <- tibble(st_str = c("american-samoa",
                                    "guam",
                                    "fsm",
                                    "nmi",
                                    "rmi",
                                    "virgin-islands"),
                      state = c("American Samoa",
                                "Guam",
                                "Federated States Of Micronesia",
                                "Northern Mariana Islands",
                                "Republic Of Marshall Islands",
                                "Virgin Islands"))

states <- bind_rows(states, territories)

# Downloading PDFs --------------------------------------------------------

# 2013 has inconsistent file structure, saved from coverage check
missing_2013 <- read_rds(here("data", "missing_2013.Rds"))

# Getting the download links for each report
href_list <- map_dfr(states$st_str, \(x){
  
  map_dfr(2010:2021, \(y){
    
    print(glue("{x} {y}"))
  
    html <- safe_read_html(
      case_when(
        y >= 2016 ~
          glue(
            "https://www.samhsa.gov/data/report/{y}-uniform-reporting-system-urs-table-{x}"
          ),
        y <= 2015 & y >= 2014 ~
          glue(
            "https://www.samhsa.gov/data/report/{y}-cmhs-uniform-reporting-system-urs-table-{x}"
          ),
        y == 2013 & !(x %in% missing_2013) ~
          glue(
            "https://www.samhsa.gov/data/report/{y}-cmhs-uniform-reporting-system-urs-tables-{x}"
          ),
        y == 2013 & x %in% missing_2013 ~
          glue(
            "https://www.samhsa.gov/data/report/{y}-cmhs-uniform-reporting-system-urs-table-{x}"
          ),
        y == 2012 ~
          glue(
            "https://www.samhsa.gov/data/report/{y}-uniform-reporting-system-urs-tables-{x}"
          ),
        y <= 2011 ~
          glue(
            "https://www.samhsa.gov/data/report/{y}-cmhs-uniform-reporting-system-urs-output-tables-{x}"
          )
      )
    )
    
    table <- tibble(
      st_str = x,
      year = y,
      st_year = glue("{x}{y}"),
      pdf_url = grab_href(html, ".download-file-link a")
    ) %>%
      mutate(
        pdf_url = glue("https://www.samhsa.gov{pdf_url}"),
        pdf_url = case_when( 
          st_str == "dc" & year == 2021 # There is an typo in the 2021 dc folder structure
          ~ "https://www.samhsa.gov/data/report/021-uniform-reporting-system-urs-table-dc",
          TRUE ~ pdf_url
        )
      ) %>%
      left_join(states)
    })
  }) %>% 
  filter(!str_detect(pdf_url, "Error"))

href_list <- href_list %>% 
  mutate(st_year = glue("{st_str}{year}"))

saveRDS(href_list, here("data", "href_list.Rds"))
beep()

href_list %<>% filter(year == 2013)

# Downloading the reports
walk2(href_list$pdf_url, href_list$st_year, \(link, st_yr){
    download.file(link,
                  destfile = here("data", "pdf", glue("samhsa_{st_yr}.pdf")),
                  mode="wb")
  })
beep()


# Scraping PDFs -----------------------------------------------------------

text_list <- list.files(path=here("data", "pdf"),
                        pattern = "samhsa.+",
                        full.names=T)

# Function to extract revenue/expenditures table
grab_rev <- possibly(function(text){
  
  print(text)

  text_matrix <- pdf_text(text) %>% 
    str_subset("State Revenue Expenditure Data") 
  
  tibble(
    state = str_extract(text_matrix, "(?<=\\n).+(?=\\n)"),
    year = str_extract(text_matrix, "[:digit:]{4}"),
    tot_smha = str_extract(text_matrix, 
                           "(?<=[:digit:]{4} Total SMHA Mental Health Expenditure).+(?=\\n)"),
    pc_smha = str_extract(text_matrix, 
                          "(?<=Per Capita Total SMHA Mental Health Expenditures).+(?=\\n)"),
    mh_blockgrant = str_extract(text_matrix, 
                                "(?<=Mental Health Block Grant).+(?=\\n)"),
    mh_community = str_extract(text_matrix, 
                               "(?<=SMHA Community MH Expenditures).+(?=\\n)"),
    mh_pc_community = str_extract(text_matrix, 
                                  "(?<=Per Capita Community MH Expenditures).+(?=\\n)"),
    mh_pct_community = str_extract(text_matrix, 
                                   "(?<=Community Percent of Total SMHA Spending).+(?=\\n)")
  ) %>% 
    mutate(across(everything(), str_squish))
  },
  otherwise = tibble(state = "Error"))

dat_samhsa_1 <- future_map_dfr(text_list, grab_rev, .progress=T) %>% 
  mutate(mh_blockgrant = str_extract(mh_blockgrant, "\\$[:graph:]*"))

# 2017 tables are different from every other year... for reasons?
grab_rev_17 <- possibly(function(text){
  
  print(text)
  
  text_matrix <- pdf_text(text) %>% 
    str_subset("State Mental Health Finance")
  
  tibble(
    state = str_extract(text_matrix, "(?<=STATE: ).+(?=\\n)"),
    year = str_extract(text_matrix, "[:digit:]{4}"),
    tot_smha = str_extract(text_matrix, 
                           "(?<=Total SMHA Expenditures).+(?=\\n)"),
    mh_community = str_extract(text_matrix, 
                               "(?<=SMHA Expenditures for Community Mental Health\\*).+(?=\\n)")
  )
}, otherwise = tibble(state = "Error"))

dat_samhsa_2 <- future_map_dfr(str_subset(text_list, "2017"), grab_rev_17, .progress=T) %>%
  mutate(
    across(.cols = tot_smha:mh_community, \(x){
      x = str_extract(x, "(?<=[:space:])[:graph:]+(?=[:space:])")}),
    across(.cols = tot_smha:mh_community, \(x){
      str_remove_all(x, "[:punct:]|\\$")})
  )

# Function for 2008-2010 tables
grab_rev_08 <- possibly(function(text){
  
  print(text)
  
  text_matrix <- pdf_text(text) %>% 
    str_subset("Basic Measure: State Mental Health Finance") 
  
  tibble(
    state = str_extract(text_matrix, "(?<=\\n).+(?=\\n)"),
    year = str_extract(text_matrix, "[:digit:]{4}"),
    tot_smha = str_extract(text_matrix, 
                           "(?<=[:digit:]{4} Total SMHA Mental Health Expenditures).+(?=\\n)"),
    pc_smha = str_extract(text_matrix, 
                          "(?<=Per Capita Total SMHA Mental Health Expenditures).+(?=\\n)"),
    mh_blockgrant = str_extract(text_matrix, 
                                "(?<=Mental Health Block Grant).+(?=\\n)"),
    mh_community = str_extract(text_matrix, 
                               "(?<=SMHA Community MH Expenditures).+(?=\\n)"),
    mh_pc_community = str_extract(text_matrix, 
                                  "(?<=Per Capita Community MH Expenditures).+(?=\\n)"),
    mh_pct_community = str_extract(text_matrix, 
                                   "(?<=Community Percent of Total SMHA Spending).+(?=\\n)")
  ) %>% 
    mutate(across(everything(), str_squish))
},
otherwise = tibble(state = "Error"))

# Grabbing 2007-2009 which came as zip files
dat_samhsa_3 <- map_dfr(2008:2009, \(x) {
  text_list <- list.files(path = here("data", "pdf", glue("{x}")),
                          full.names = T)
  future_map_dfr(text_list, grab_rev_08, .progress = T)
}) %>%
  mutate(mh_blockgrant = str_extract(mh_blockgrant, "\\$[:graph:]*"))

# Function for 2007 tables
grab_rev_07 <- possibly(function(text){
  
  print(text)
  
  text_matrix <- pdf_text(text) %>% 
    str_subset("TABLE 2: State Mental Health Agency Controlled Revenues by Funding Source, FY 2005") 
  
  tibble(
    state = str_extract(text_matrix, "(?<=\\n).+(?=\\n)"),
    year = str_extract(text_matrix, "[:digit:]{4}"),
    tot_smha = str_extract(text_matrix, 
                           "(?<=[:digit:]{4} Total SMHA Mental Health Expenditures).+(?=\\n)"),
    pc_smha = str_extract(text_matrix, 
                          "(?<=Per Capita Total SMHA Mental Health Expenditures).+(?=\\n)"),
    mh_blockgrant = str_extract(text_matrix, 
                                "(?<=Mental Health Block Grant).+(?=\\n)"),
    mh_community = str_extract(text_matrix, 
                               "(?<=SMHA Community MH Expenditures).+(?=\\n)"),
    mh_pc_community = str_extract(text_matrix, 
                                  "(?<=Per Capita Community MH Expenditures).+(?=\\n)"),
    mh_pct_community = str_extract(text_matrix, 
                                   "(?<=Community Percent of Total SMHA Spending).+(?=\\n)")
  ) %>% 
    mutate(across(everything(), str_squish))
},
otherwise = tibble(state = "Error"))

# Grabbing 2007 which came as zip files
dat_samhsa_3 <- map_dfr(2008:2009, \(x) {
  text_list <- list.files(path = here("data", "pdf", glue("{x}")),
                          full.names = T)
  future_map_dfr(text_list, grab_rev_07, .progress = T)
}) %>%
  mutate(mh_blockgrant = str_extract(mh_blockgrant, "\\$[:graph:]*"))


    
# Cleaning & Saving -------------------------------------------------------

# Adding labels and getting numeric data from strings
dat_samhsa <- dat_samhsa_1 %>% 
  bind_rows(dat_samhsa_2, dat_samhsa_3) %>% 
  mutate(
    across(.cols = tot_smha:mh_pct_community, 
           \(x) str_remove_all(x, "[:punct:]|\\$")),
    across(.cols = year:mh_pct_community,
           as.numeric)
  ) %>% 
  set_variable_labels(
    tot_smha = "Total SMHA Mental Health Expenditure ($)",
    pc_smha = "Per Capita Total SMHA Mental Health Expenditures ($)",
    mh_blockgrant = "Mental Health Block Grant Expenditures ($)",
    mh_pc_community = "Per Capita Community MH Expenditures ($)",
    mh_pct_community = "Community Percent of Total SMHA Spending (%)"
  ) %>% 
  arrange(state, year)

# Saving
saveRDS(dat_samhsa, here("data", "samhsa_data_2010_2011.Rds"))


# Coverage Check ---------------------------------------------------------

coverage <- dat_samhsa %>% 
  group_by(state) %>% 
  summarize(cov = list(unique(year)),
            cov_n = length(unlist(cov)))

full_cov <- tibble(state = unique(states$state)) %>% 
  expand(state, year=2007:2021)

missing_href <- anti_join(full_cov, href_list)
# Gonna grab the last of these by hand

missing_final <- anti_join(full_cov, dat_samhsa) %>% 
  filter(!(state %in% territories$state))

missing %<>% 
  str_to_lower() %>% 
  str_replace_all(., " ", "-")

saveRDS(missing, here("data", "missing_2013.Rds"))



