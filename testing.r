library(readr)
library(dplyr)
library(tidyr)
library(httr)
library(jsonlite)

# --- CONFIGURATION ---
LLM_MODEL_NAME     <- "grok-4-1-fast-non-reasoning" 
# LLM_MODEL_NAME   <- "grok-beta" # Fallback if the above ID stops working
API_KEY_ENV_VAR    <- "xai-6lWWSzmfBr6wYeS2U0gFJpKfC4rQCgD46n4RokdNd4BbApqp8IicnyJKzETOAXmaFyyc06Ujn3EWK81F"

INPUT_CSV          <- "sentiment_analysis/all_news_final.csv"
CHECKPOINT_RDS     <- "sentiment_analysis/all_news_with_sentiment_checkpoint.rds"
OUTPUT_ARTICLE_CSV <- "sentiment_analysis/all_news_with_sentiment.csv"
OUTPUT_EVENT_CSV   <- "sentiment_analysis/event_level_sentiment.csv"

# --- 0. LOAD DATA ---
if(!file.exists(INPUT_CSV)) stop("Input CSV not found!")
all_news_final <- readr::read_csv(INPUT_CSV, show_col_types = FALSE)

# 1. Align names
all_news_final <- all_news_final %>%
  rename(earningsid = earnings_id, period = relative_to_earnings) %>%
  mutate(scraped_text = as.character(scraped_text))

# 2. Ensure unique_row_id
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

# 4. Initialize Columns (These are the strict keys we enforce)
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

for (col in json_boolean_keys) {
  if (!col %in% colnames(all_news_final)) all_news_final[[col]] <- NA
}
# Mirror column
if (!"sentiment_signal" %in% colnames(all_news_final)) all_news_final$sentiment_signal <- NA

# --- API FUNCTION ---
get_batch_sentiment <- function(batch_df, model = LLM_MODEL_NAME, api_key = API_KEY_ENV_VAR) {
  
  articles_list <- batch_df %>%
    select(id = unique_row_id, text = scraped_text) %>%
    filter(!is.na(text), nzchar(text)) %>%
    mutate(text = substr(text, 1, 10000)) 
  
  if (nrow(articles_list) == 0) {
    warning("Batch skipped: No valid text.")
    return(NULL)
  }
  
  json_input <- toJSON(articles_list, auto_unbox = TRUE)
  
  # --- CRITICAL FIX: DYNAMICALLY INSERT KEYS INTO PROMPT ---
  # We force the LLM to use exactly the keys in our R dataframe
  keys_string <- paste(json_boolean_keys, collapse = ", ")
  
  system_prompt <- sprintf("
  You are an equity research assistant. 
  Analyze EACH article in the provided JSON list.
  
  Your Output must be a JSON object:
  {
    \"results\": [
      {
        \"id\": (integer from input),
        ... (boolean flags) ...
      }
    ]
  }

  STRICT RULES:
  1. Return ONLY Valid JSON. No markdown formatting.
  2. For every article, you MUST include these exact boolean (0/1) keys: 
     [%s]
  3. 'overall_good_stock_narrative' should be 1 if the sentiment is fundamentally positive.
  ", keys_string)
  
  payload <- list(
    model = model,
    messages = list(
      list(role = "system", content = system_prompt),
      list(role = "user",   content = paste("Analyze this list:", json_input))
    ),
    temperature = 0.0
  )
  
  cat("Sending request to API...\n")
  
  res <- POST(
    url = "https://api.x.ai/v1/chat/completions",
    add_headers(Authorization = paste("Bearer", api_key)),
    body = payload,
    encode = "json"
  )
  
  if (status_code(res) != 200) {
    warning(sprintf("API Error [%d]: %s", status_code(res), content(res, "text", encoding = "UTF-8")))
    return(NULL)
  }
  
  content_str <- content(res, as = "text", encoding = "UTF-8")
  
  tryCatch({
    # 1. Parse the outer API response
    # Clean just in case, though usually not needed for the outer wrapper
    api_json <- fromJSON(content_str)
    
    # --- CRITICAL FIX: Correctly access the message content ---
    # jsonlite parses 'choices' as a data.frame. We need the first row's message$content.
    if ("choices" %in% names(api_json) && "message" %in% names(api_json$choices)) {
       raw_llm_text <- api_json$choices$message$content[1]
    } else {
       # Fallback if structure is different
       raw_llm_text <- api_json$choices[[1]]$message$content
    }

    # 2. Clean the inner text (remove markdown ```json ... ```)
    inner_cleaned <- gsub("^```json\\s*|\\s*```$", "", raw_llm_text)
    inner_cleaned <- gsub("^```\\s*|\\s*```$", "", inner_cleaned)
    
    # 3. Parse the results
    result_data <- fromJSON(inner_cleaned)
    return(result_data$results)
    
  }, error = function(e) {
    warning("JSON Parse Error: ", conditionMessage(e))
    cat("DEBUG RAW RESPONSE:\n", substr(content_str, 1, 500), "\n") 
    return(NULL)
  })
}

# --- 5. EXECUTION LOOP (TESTING ROW 3) ---

# Select Row 3
test_batch <- all_news_final %>% slice(3)

# Double check it has text
if(is.na(test_batch$scraped_text) || test_batch$scraped_text == "") {
  message("Row 3 is empty. Trying to find a row with text...")
  test_batch <- all_news_final %>% filter(!is.na(scraped_text) & nchar(scraped_text) > 50) %>% slice(1)
}

batches_to_process <- test_batch %>% pull(batch_id) %>% unique()

cat(sprintf("Testing on %d batch (ID: %s)...\n", length(batches_to_process), batches_to_process))

for (b_id in batches_to_process) {
  
  batch_slice <- test_batch %>% filter(batch_id == b_id)
  llm_results <- get_batch_sentiment(batch_slice)
  
  if (!is.null(llm_results) && nrow(llm_results) > 0) {
    
    print("Success! Results received:")
    print(head(llm_results)) # Print first few cols
    
    llm_results$id <- as.integer(llm_results$id)
    
    for (r in seq_len(nrow(llm_results))) {
      row_res   <- llm_results[r, ]
      target_id <- row_res$id
      
      idx <- which(test_batch$unique_row_id == target_id)
      if (length(idx) > 0) {
        for (key in json_boolean_keys) {
          # Only fill if the key actually exists in response
          if (key %in% names(row_res)) {
            test_batch[[key]][idx] <- as.integer(row_res[[key]])
          }
        }
      }
    }
  } else {
    cat(" -> Batch failed.\n")
  }
}

# Check if data filled
print(test_batch %>% select(unique_row_id, strong_financial_metrics, overall_good_stock_narrative))