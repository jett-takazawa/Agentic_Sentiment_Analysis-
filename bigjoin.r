# 1. Load the Sentiment Data
# (I changed IDs to MMM so they match the earnings data below for this demo)

df_sentiment <- read.csv("sentiment_composite_new_weights.csv")

df_earnings <- read.csv("earnings_with_financials2.csv")


# 4. Prepare Sentiment Data for Merge
# We separate PRE and POST into subsets to join them as columns
sent_pre <- df_sentiment[df_sentiment$period == "PRE", c("earningsid", "composite_score")]
names(sent_pre)[2] <- "sentiment_prior"

sent_post <- df_sentiment[df_sentiment$period == "POST", c("earningsid", "composite_score")]
names(sent_post)[2] <- "sentiment_post"

# 5. Join to Earnings Data
# We use all.x=TRUE (Left Join) to keep all earnings records even if sentiment is missing
# Note: Earnings data uses "earnings_id", Sentiment uses "earningsid"
final_df <- merge(df_earnings, sent_pre, by.x = "earnings_id", by.y = "earningsid", all.x = TRUE)
final_df <- merge(final_df, sent_post, by.x = "earnings_id", by.y = "earningsid", all.x = TRUE)

# 6. View Results
# Display specific columns to verify the join
cols_to_view <- c("earnings_id", "reportedDate", "sentiment_prior", "sentiment_post")
print(head(final_df[, cols_to_view]))

# Save Final
write.csv(final_df, "earnings_with_sentiment_scores_weights.csv", row.names = FALSE)