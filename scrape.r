library(dplyr)
library(httr)
library(rvest)
library(xml2)
library(dplyr)

library(readr)  # or use base read.csv
news_capped <- read_csv("news_capped.csv")
scrape_article_text <- function(
  url,
  selectors = NULL,
  timeout_sec = 10, # Reduced timeout to fail faster on bad links
  user_agent_str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
  min_chars_fallback = 200
) {
  # Safety check for empty or NA URLs
  if (is.na(url) || !nzchar(url)) return(NA_character_)

  # Wrap entire logic in tryCatch to handle HTTP errors/Timeouts gracefully
  tryCatch({
    # 1) Fetch HTML
    resp <- httr::GET(
      url,
      httr::user_agent(user_agent_str),
      httr::timeout(timeout_sec)
      # accept("text/html") removed to be more permissive
    )

    # If status is not 200 (OK), return NA immediately
    if (httr::status_code(resp) != 200) return(NA_character_)

    doc <- xml2::read_html(httr::content(resp, as = "raw"))

    # 2) Remove non-content noise
    remove_xpaths <- c(
      "//script", "//style", "//noscript", "//header", "//footer",
      "//nav", "//form", "//aside", "//figure", "//figcaption",
      "//svg", "//iframe", "//img", "//picture", "//button",
      "//video", "//audio", "//div[contains(@class, 'share')]",
      "//div[contains(@class, 'related')]"
    )
    for (xp in remove_xpaths) {
      xml2::xml_remove(xml2::xml_find_all(doc, xp))
    }

    # 3) Candidate selectors
    default_selectors <- c(
      'article [itemprop="articleBody"]', 'article .article-body',
      'main [itemprop="articleBody"]', 'main .article-body',
      'div[itemprop="articleBody"]', '.article-body',
      '.article__content', '.post-content', '.entry-content',
      '#article-body', '#articleBody', 'article', 'main article',
      'section[itemprop="articleBody"]', 'section.article-body',
      'article .content'
    )
    cand_selectors <- if (is.null(selectors)) default_selectors else c(selectors, default_selectors)

    # 4) Helper functions
    score_node <- function(node) {
      ps <- rvest::html_elements(node, "p")
      if (length(ps) == 0) return(list(nchar = 0L, text = character(0)))
      txt <- rvest::html_text2(ps)
      txt <- txt[nzchar(txt)]
      list(nchar = nchar(paste(txt, collapse = " ")), text = txt)
    }

    pick_best_from_nodes <- function(nodeset) {
      best_len <- -1L
      best_txt <- NULL
      for (nd in nodeset) {
        sc <- score_node(nd)
        if (sc$nchar > best_len) {
          best_len <- sc$nchar
          best_txt <- sc$text
        }
      }
      list(nchar = best_len, text = best_txt)
    }

    # 5) Try CSS selectors
    best <- list(nchar = -1L, text = character(0))
    for (sel in cand_selectors) {
      nodes <- rvest::html_elements(doc, sel)
      if (length(nodes) == 0) next
      candidate <- pick_best_from_nodes(nodes)
      if (candidate$nchar > best$nchar) best <- candidate
    }

    # 6) Fallback: largest text block
    if (best$nchar < min_chars_fallback) {
      candidates <- rvest::html_elements(
        doc,
        xpath = '//*[self::article or self::div or self::section or self::main][count(.//p) > 1]'
      )
      if (length(candidates) > 0) {
        candidate <- pick_best_from_nodes(candidates)
        if (candidate$nchar > best$nchar) best <- candidate
      }
    }

    # 7) Last-resort fallback
    if (best$nchar < min_chars_fallback) {
      body_ps <- rvest::html_elements(doc, "body p")
      if (length(body_ps) > 0) {
        txt <- rvest::html_text2(body_ps)
        txt <- txt[nzchar(txt)]
        best <- list(nchar = nchar(paste(txt, collapse = " ")), text = txt)
      }
    }

    # 8) Normalize
    if (length(best$text) == 0) return(NA_character_)

    norm <- function(x) {
      x <- gsub("[ \t]+", " ", x)
      trimws(x)
    }
    best$text <- vapply(best$text, norm, character(1))
    best$text <- best$text[nzchar(best$text)]
    paste(best$text, collapse = "\n\n")

  }, error = function(e) {
    # On ANY error, return NA so the loop continues
    return(NA_character_)
  })
}






# 1. Extract unique URLs
unique_url_df <- news_capped |>
  distinct(url) |>
  filter(!is.na(url), url != "")

cat("Total rows in news_capped:", nrow(news_capped), "\n")
cat("Unique URLs to scrape:", nrow(unique_url_df), "\n")

# 2. Initialize column
unique_url_df$scraped_text <- NA_character_

total   <- nrow(unique_url_df)
start_t <- Sys.time()

# --- SAFETY ADDITION 1: Define a temporary save file ---
temp_file <- "news_scrape_checkpoint.rds"

if (total == 0) {
  warning("No valid URLs found in news_capped.")
} else {
  
  # --- SAFETY ADDITION 2: Resume capability (Optional but helpful) ---
  if (file.exists(temp_file)) {
    message("Found checkpoint file! Resuming...")
    unique_url_df <- readRDS(temp_file)
    # Start at the first NA row
    first_na <- min(which(is.na(unique_url_df$scraped_text)))
    # If all done, start from total + 1 to skip loop
    start_index <- if (is.finite(first_na)) first_na else (total + 1) 
  } else {
    start_index <- 1
  }

  cat("Starting scrape at", format(start_t), "from index", start_index, "\n")

  # Loop
  for (i in start_index:total) {
    # If we are already past the end (because of resume), break
    if (i > total) break 
    
    current_url <- unique_url_df$url[i]

    # Progress logging
    if (i %% 50 == 0 || i == 1 || i == total) {
      elapsed <- as.numeric(difftime(Sys.time(), start_t, units = "secs"))
      pct     <- (i / total) * 100
      cat(sprintf(
        "[%s] Processing %d of %d (%.1f%%) — elapsed: %.1fs\n",
        format(Sys.time(), "%H:%M:%S"),
        i, total, pct, elapsed
      ))
      
      # --- SAFETY ADDITION 3: Save to disk every 50-100 rows ---
      # This ensures if R crashes, you only lose the last few minutes of work.
      saveRDS(unique_url_df, temp_file)
    }

    # Scrape this URL
    # CRITICAL: Ensure scrape_article_text() has the tryCatch block inside it!
    txt <- scrape_article_text(current_url) 
    unique_url_df$scraped_text[i] <- txt

    # Politeness sleep: ~0.25s on average
    # This is fast/aggressive. If you get 429/403 errors, increase this.
    pause <- runif(1, 0.15, 0.4)
    Sys.sleep(pause)
  }

  end_t <- Sys.time()
  cat("Finished scrape at", format(end_t), "\n")
  cat("Total elapsed:",
      round(as.numeric(difftime(end_t, start_t, units = "mins")), 2),
      "minutes\n")

}

# 4. Join scraped text back to the capped news set
all_news_final <- news_capped |>
  left_join(unique_url_df, by = "url")


readr::write_csv(all_news_final, "sentiment_analysis/all_news_final.csv")




