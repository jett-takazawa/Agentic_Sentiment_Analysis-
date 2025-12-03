library(readr)
library(dplyr)
library(tidyr)
library(httr)
library(jsonlite)

# --- CONFIGURATION ---
LLM_MODEL_NAME     <- "grok-4-1-fast-non-reasoning" 
API_KEY_ENV_VAR    <- "xai-"
INPUT_CSV          <- "sentiment_analysis/all_news_final.csv"
CHECKPOINT_RDS     <- "sentiment_analysis/all_news_with_sentiment_checkpoint.rds"
OUTPUT_ARTICLE_CSV <- "sentiment_analysis/all_news_with_sentiment.csv"
OUTPUT_EVENT_CSV   <- "sentiment_analysis/event_level_sentiment.csv"

# --- 0. LOAD DATA ---

if(!file.exists(INPUT_CSV)) stop("Input CSV not found!")

all_news_final <- readr::read_csv(INPUT_CSV, show_col_types = FALSE)

# 1. Align names
all_news_final <- all_news_final %>%
  rename(
    earningsid = earnings_id,
    period     = relative_to_earnings
  ) %>%
  mutate(
    scraped_text = as.character(scraped_text)
  )

# 2. Ensure unique_row_id exists
if (!"unique_row_id" %in% colnames(all_news_final)) {
  all_news_final$unique_row_id <- as.integer(seq_len(nrow(all_news_final)))
}

# 3. Create batches
all_news_final <- all_news_final %>%
  group_by(earningsid, period) %>% 
  mutate(
    batch_index = ceiling(row_number() / 25),
    batch_id    = paste(earningsid, period, batch_index, sep = "_")
  ) %>%
  ungroup()

# 4. Initialize Columns
# These are the detailed flags we expect from JSON
json_boolean_keys <- c(
  "strong_financial_metrics",
  "revenue_profit_growth",
  "reasonable_valuation",
  "healthy_roe",
  "strong_cashflow_low_debt",
  "positive_dividend_history",
  "quality_management",
  "competitive_advantage_moat",
  "industry_leadership_and_trends",
  "new_products_or_services",
  "clear_positive_rating",
  "risk_assessment_present",
  "forward_looking_outlook",
  "overall_good_stock_narrative" 
)

# Initialize them in DF as NA
for (col in json_boolean_keys) {
  if (!col %in% colnames(all_news_final)) all_news_final[[col]] <- NA
}

# We also initialize sentiment_signal which will mirror overall_good_stock_narrative
if (!"sentiment_signal" %in% colnames(all_news_final)) all_news_final$sentiment_signal <- NA


# --- API FUNCTION ---
get_batch_sentiment <- function(batch_df, model = LLM_MODEL_NAME, api_key = API_KEY_ENV_VAR) {
  
  # --- TELEMETRY: CHECK INPUT ---
  articles_list <- batch_df %>%
    select(id = unique_row_id, text = scraped_text) %>%
    filter(!is.na(text), nzchar(text)) %>%
    # REDUCE TEXT SIZE FOR SAFETY: Lower to 2000 chars for now to test
    mutate(text = substr(text, 1, 2000)) 
  
  num_articles <- nrow(articles_list)
  total_chars  <- sum(nchar(articles_list$text))
  
  cat(sprintf("   > Preparing API Payload: %d articles, ~%d total chars.\n", num_articles, total_chars))
  
  if (num_articles == 0) {
    cat("   > SKIPPING: No valid text in this batch.\n")
    return(NULL)
  }
  
  # Check for massive payloads that will crash the API
  if (total_chars > 200000) {
    warning("   > WARNING: Payload is massive (>200k chars). This will likely fail.")
  }

  json_input <- toJSON(articles_list, auto_unbox = TRUE)
  keys_string <- paste(json_boolean_keys, collapse = ", ")
  
  system_prompt <- sprintf("
  You are an equity research assistant. Analyze EACH article.
  STRICT JSON OUTPUT: {\"results\": [{\"id\": 123, ...}]}
  REQUIRED BOOLEAN KEYS: [%s]
  'overall_good_stock_narrative': 1 if positive.
  ", keys_string)
  
  payload <- list(
    model = model,
    messages = list(
      list(role = "system", content = system_prompt),
      list(role = "user",   content = paste("Analyze:", json_input))
    ),
    temperature = 0.0
  )
  
  cat("   > Sending request to xAI... ")
  
  res <- POST(
    url = "https://api.x.ai/v1/chat/completions",
    add_headers(Authorization = paste("Bearer", api_key)),
    body = payload,
    encode = "json",
    timeout(60) # Add a 60-second timeout
  )
  
  # --- TELEMETRY: CHECK STATUS ---
  status <- status_code(res)
  cat(sprintf("[Status: %d]\n", status))
  
  if (status != 200) {
    # PRINT THE ACTUAL ERROR MESSAGE FROM GROK
    error_msg <- content(res, "text", encoding = "UTF-8")
    cat(sprintf("   > API ERROR DETAILS: %s\n", substr(error_msg, 1, 500)))
    return(NULL)
  }
  
  content_str <- content(res, as = "text", encoding = "UTF-8")
  
  tryCatch({
    api_json <- fromJSON(content_str)
    
    if ("choices" %in% names(api_json) && "message" %in% names(api_json$choices)) {
       raw_llm_text <- api_json$choices$message$content[1]
    } else {
       raw_llm_text <- api_json$choices[[1]]$message$content
    }

    inner_cleaned <- gsub("^```json\\s*|\\s*```$", "", raw_llm_text)
    inner_cleaned <- gsub("^```\\s*|\\s*```$", "", inner_cleaned)
    
    result_data <- fromJSON(inner_cleaned)
    
    cat(sprintf("   > Success! Parsed %d results.\n", nrow(result_data)))
    return(result_data$results)
    
  }, error = function(e) {
    cat(sprintf("   > JSON PARSE FAILURE: %s\n", conditionMessage(e)))
    cat(sprintf("   > RAW RESPONSE START: %s\n", substr(content_str, 1, 200)))
    return(NULL)
  })
}
# --- 5. EXECUTION LOOP ---
batches_to_process <- all_news_final %>%
  filter(is.na(overall_good_stock_narrative), !is.na(scraped_text)) %>%
  pull(batch_id) %>%
  unique()

total_batches <- length(batches_to_process)
cat(sprintf("Found %d batches to process.\n", total_batches))

counter <- 0

for (b_id in batches_to_process) {
  counter <- counter + 1
  
  batch_slice <- all_news_final %>% filter(batch_id == b_id)
  
  cat(sprintf("[%d/%d] Processing batch %s (%d articles)...\n",
              counter, total_batches, b_id, nrow(batch_slice)))
  
  llm_results <- get_batch_sentiment(batch_slice)
  
  if (!is.null(llm_results) && nrow(llm_results) > 0) {
    
    llm_results$id <- as.integer(llm_results$id)
    
    for (r in seq_len(nrow(llm_results))) {
      row_res   <- llm_results[r, ]
      target_id <- row_res$id
      
      idx <- which(all_news_final$unique_row_id == target_id)
      
      if (length(idx) > 0) {
        # Map all boolean keys
        for (key in json_boolean_keys) {
          if (key %in% names(row_res)) {
            all_news_final[[key]][idx] <- as.integer(row_res[[key]])
          }
        }
      }
    }
  } else {
    cat("  -> Batch failed/empty.\n")
  }
  
  Sys.sleep(0.5)
  if (counter %% 10 == 0) saveRDS(all_news_final, CHECKPOINT_RDS)
}

# Sync sentiment_signal to overall_good_stock_narrative
all_news_final$sentiment_signal <- all_news_final$overall_good_stock_narrative

readr::write_csv(all_news_final, OUTPUT_ARTICLE_CSV)

# --- 6. EVENT-LEVEL AGGREGATION ---

event_sentiment <- all_news_final %>%
  group_by(earningsid, period) %>%
  summarise(
    # Aggregate booleans: if ANY article is 1, the event is 1
    across(
      all_of(json_boolean_keys),
      ~ as.integer(max(., na.rm = TRUE) == 1),
      .names = "{.col}"
    ),
    # Ensure sentiment_signal is also aggregated
    sentiment_signal = as.integer(max(sentiment_signal, na.rm = TRUE) == 1),
    
    .groups = "drop"
  )

# Replace -Inf (from all-NA groups) with 0
event_sentiment[is.na(event_sentiment)] <- 0

readr::write_csv(event_sentiment, OUTPUT_EVENT_CSV)
cat("Done! Files saved.\n")