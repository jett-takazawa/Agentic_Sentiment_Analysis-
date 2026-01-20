# Agentic Sentiment Analysis

An innovative sentiment analysis system that leverages Large Language Models (LLMs) to generate 15 binary sentiment outputs from financial news articles, combined with linear regression to create optimized composite sentiment scores for predicting stock price movements around earnings announcements.

## Overview

This project introduces a novel approach to financial sentiment analysis by:

1. **Scraping ~14,000 news articles** around earnings announcement dates using Alpha Vantage API and rvest
2. **Using GROK 4.1 as an agentic LLM** to analyze each article and return 15 distinct binary sentiment indicators
3. **Applying linear regression** to learn optimal weights for combining these binary outputs into a single composite sentiment score
4. **Correlating sentiment with stock price movements** during earnings periods

## Key Innovation: 15 Binary Sentiment Outputs

Instead of traditional single-score sentiment analysis, this system prompts the LLM to evaluate articles across 15 different sentiment dimensions, each returning a binary (0/1) output. This multi-dimensional approach captures nuanced aspects of financial sentiment that single-score methods miss.

The linear regression model (`linearregression_weights_GEMINI.ipynb`) then learns the optimal weights for each binary feature based on their predictive power for stock price movements.

## Project Structure

```
├── Group_9_DataFetch.ipynb          # Data fetching from Alpha Vantage API
├── scrape.r                          # Web scraping with rvest
├── LLM.r                             # GROK 4.1 API integration for sentiment analysis
├── linearregression_weights_GEMINI.ipynb  # Linear regression for weight optimization
├── sentiment_composite.r             # Composite score calculation
├── bigjoin.r                         # Data joining operations
├── newweights.r                      # Updated weight calculations
├── testing.r                         # Model testing and validation
├── save.r                            # Data persistence utilities
├── recovery.ipynb                    # Data recovery scripts
│
├── all_earnings.csv                  # Raw earnings data
├── news_capped.csv                   # Scraped news articles
├── earnings_with_financials.csv      # Earnings with financial data
├── earnings_with_sentiment_scores.csv        # Sentiment analysis results
├── earnings_with_sentiment_scores_weights_unbias.csv  # Unbiased weighted scores
├── sentiment_composite.csv           # Final composite sentiment scores
├── sentiment_composite_new_weights.csv       # Scores with optimized weights
├── data_final.csv                    # Final processed dataset
│
├── news_scrape_checkpoint.rds        # Scraping checkpoint for recovery
└── sentiment_analysis.Rproj          # RStudio project file
```

## Tech Stack

- **R** - Primary analysis language
  - `rvest` - Web scraping
  - `tidyverse` - Data manipulation
  - `httr` / API packages - API interactions
  
- **Python** (Jupyter Notebooks)
  - Data fetching
  - Linear regression modeling
  
- **APIs**
  - Alpha Vantage - Financial data and news
  - GROK 4.1 (xAI) - Agentic LLM for sentiment analysis

## Methodology

### 1. Data Collection
- Fetch earnings announcement dates for target companies
- Scrape news articles from the days surrounding each earnings event
- Collect corresponding stock price data

### 2. Agentic Sentiment Analysis
- Each article is processed through GROK 4.1
- The LLM evaluates the article across 15 binary sentiment dimensions
- Results are structured as a vector of 0s and 1s

### 3. Weight Optimization
- Linear regression identifies which binary features best predict price movements
- Weights are learned from historical data
- Unbiased weight calculations account for data distribution

### 4. Composite Score Generation
- Binary outputs are combined using learned weights
- Final composite score provides a single, optimized sentiment metric

## Usage

1. **Set up API keys** for Alpha Vantage and GROK

2. **Fetch earnings data:**
   ```r
   # Run Group_9_DataFetch.ipynb or equivalent R scripts
   ```

3. **Scrape news articles:**
   ```r
   source("scrape.r")
   ```

4. **Run sentiment analysis:**
   ```r
   source("LLM.r")
   ```

5. **Optimize weights:**
   ```python
   # Run linearregression_weights_GEMINI.ipynb
   ```

6. **Generate composite scores:**
   ```r
   source("sentiment_composite.r")
   ```

## Results

The agentic approach with 15 binary outputs and learned weights has demonstrated improved predictive capability compared to traditional single-score sentiment methods for earnings-related price movements.

## Requirements

### R Packages
```r
install.packages(c("rvest", "tidyverse", "httr", "jsonlite"))
```

### Python Packages
```bash
pip install pandas numpy scikit-learn jupyter
```

### API Keys Required
- [Alpha Vantage](https://www.alphavantage.co/) - Free tier available
- [xAI GROK](https://x.ai/grok) - For LLM sentiment analysis

## License

This project is for research and educational purposes.

## Acknowledgments

- Alpha Vantage for financial data APIs
- xAI for GROK LLM access
