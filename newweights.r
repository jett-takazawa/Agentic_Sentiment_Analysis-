# 1. Load the data
df <- read.csv("sentiment_analysis/event_level_sentiment.csv")

# 2. Define Weights
# I have entered the raw values you provided. 
# The script will verify these names exist in your CSV and normalize the math.
raw_weights <- c(
  "forward_looking_outlook"        = -2.5084,
  "clear_positive_rating"          = -2.1337,
  "new_products_or_services"       = -1.0833,
  "positive_dividend_history"      = -0.6770,
  "competitive_advantage_moat"     = -0.5809,
  "risk_assessment_present"        = -0.2397,
  "revenue_profit_growth"          =  0.7425,
  "strong_financial_metrics"       =  0.7877,
  "healthy_roe"                    =  0.8557,
  "reasonable_valuation"           =  1.4057,
  "quality_management"             =  1.6436,
  "strong_cashflow_low_debt"       =  2.1305,
  "overall_good_stock_narrative"   =  2.2343,
  "sentiment_signal"               =  2.2343,
  "industry_leadership_and_trends" =  2.6164
)

# 3. Process Weights (Coefficients of 1)
# Calculate the sum of absolute values to normalize
total_weight <- sum(abs(raw_weights))
norm_weights <- raw_weights / total_weight

# Check alignment: Ensure these columns actually exist in the dataframe
# This looks for the names in 'raw_weights' inside your loaded dataframe
cols_to_score <- names(norm_weights)
missing_cols <- setdiff(cols_to_score, names(df))

if(length(missing_cols) > 0) {
  stop(paste("Error: The following columns are missing from the CSV:", paste(missing_cols, collapse=", ")))
}

# 4. Calculate the Weighted Composite Score
# We multiply the dataframe columns by the normalized weights and sum the rows.
# 'sweep' multiplies each column by its corresponding weight efficiently.
weighted_data <- sweep(df[, cols_to_score], 2, norm_weights, "*")

# Sum the weighted values to get the final score
df$composite_score <- rowSums(weighted_data, na.rm = TRUE)

# 5. View and Save
# Print the first few rows to verify
head(df[c("composite_score", cols_to_score)])

# Save the full combined dataset to a new file:
write.csv(df, "sentiment_composite_new_weights.csv", row.names = FALSE)