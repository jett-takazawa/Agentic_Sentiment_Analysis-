# 1. Load the data
# We read the data directly from the text string for this example.
df <- read.csv("sentiment_analysis/event_level_sentiment.csv")


# 2. Identify Numeric Columns for the Composite Score
# We want columns 3 through 17 (excluding earningsid and period)
# You can also select by name if the index might change:
# numeric_cols <- sapply(df, is.numeric)
cols_to_score <- 3:17

# 3. Calculate the Composite Score
# We use rowMeans to get an equal-weighted score between 0 and 1.
# If you prefer a raw sum, use rowSums().
df$composite_score <- rowMeans(df[, cols_to_score], na.rm = TRUE)

# Optional: Format as a percentage for readability
# 4. View the result
# The new 'composite_score' column is now effectively joined to the original data
# We print the first few rows of the ENTIRE dataframe to show all columns + scores


# To save the full combined dataset to a new file:
write.csv(df, "sentiment_composite_new_weights.csv", row.names = FALSE)