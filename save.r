library(dplyr)
library(httr)
library(rvest)
library(xml2)
library(dplyr)

library(readr)  # or use base read.csv


unique_url_df <- readRDS("news_scrape_checkpoint.rds")
all_news_final <- news_capped |>
  dplyr::left_join(unique_url_df, by = "url")

if (!dir.exists("sentiment_analysis")) {
  dir.create("sentiment_analysis", recursive = TRUE)
}

readr::write_csv(all_news_final, "sentiment_analysis/all_news_final.csv")