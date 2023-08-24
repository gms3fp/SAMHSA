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
library(furrr)
library(tictoc)
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
         st_str = str_replace(st_str, " ", "-"),
         st_str = case_when(
           st_str == "district-of columbia" ~ "dc",
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

# 2016-2021 (same folder structure)

# Getting pdf links from each page (the pdf links are weirdly organized)
href_list <- map_dfr(states$st_str[1:2], \(x){
  
  map_dfr(2016:2021, \(y){
    
    html <- safe_read_html(
      glue("https://www.samhsa.gov/data/report/{y}-uniform-reporting-system-urs-table-{x}"))
    
    table <- tibble(
      st_str = x,
      year = y,
      landing_url =
        glue(
          "https://www.samhsa.gov/data/report/2021-uniform-reporting-system-urs-table-{x}"
        ),
      pdf_url = grab_href(html, ".download-file-link a")
    ) %>%
      mutate(
        pdf_url = glue("https://www.samhsa.gov{pdf_url}"),
        pdf_url = case_when( 
          st_str == "dc" & year == 2021 # There is an error w/ the 2021 dc folder structure
          ~ "https://www.samhsa.gov/data/report/021-uniform-reporting-system-urs-table-dc",
          TRUE ~ pdf_url
        )
      ) %>%
      left_join(states)
    })
  }) 

# Finally Download
walk2(href_list$pdf_url, href_list$year, \(link, yr){
    state <- str_extract(link, "(?<=[:digit:]/).+(?=\\.pdf)")
    download.file(link,
                  destfile = here("data", glue("samhsa_{state}_{yr}.pdf")),
                  mode="wb")
  })


# Scraping PDFs -----------------------------------------------------------

text <- pdf_text(here("data", "samhsa_Alabama_2020.pdf"))
text_split <- str_split(text, "\n") %>% 
  str_trim()
?str_split
text_split[[3]]










# Junkyard ----------------------------------------------------------------


str_extract(href_list$pdf_url[1], "(?<=[:digit:]/).+(?=\\.pdf)")

year <- str_extract(href_list$pdf_url[1], "[:digit:]{2}(?=[:punct:]uniform)")

test <- tibble(link = grab_href(html, ".download-file-link a")) %>% 
  mutate(link = glue("https://www.samhsa.gov/{link}"))

download.file(href_list$pdf_url[1],
              destfile=here("data", "test.pdf"),
              mode="wb")

href_list$pdf_url[1]

# https://www.samhsa.gov/data/report/2021-uniform-reporting-system-urs-table-arizona

test <- tibble(x = 1:20)
