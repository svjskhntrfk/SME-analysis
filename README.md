# SME Industry Analysis

A comprehensive tool for analyzing Small and Medium-sized Enterprises (SMEs) in Russia to identify the most promising industries for starting a business. The project includes data collection through web scraping and sophisticated analysis of business metrics.

## 🚀 Features

- Automated web scraping of company data from RBC (companies.rbc.ru)
- Collection of detailed company information including:
  - Revenue
  - Growth rates
  - OKVED codes (Russian industry classification)
  - Company ownership
- Statistical analysis of industry performance
- Outlier detection and data cleaning
- Generation of detailed Excel reports
- Visualization of industry distributions

## 🛠 Technology Stack

- **Python**
- **Key Libraries**:
  - `selenium` - Web scraping
  - `aiosqlite` - Async SQLite database operations
  - `pandas` - Data analysis
  - `numpy` - Numerical operations
  - `matplotlib` & `seaborn` - Data visualization
  - `scipy` - Statistical analysis

## 📊 Data Analysis Features

- Removal of statistical outliers using z-score method
- Industry-specific analysis including:
  - Average revenue and growth rates
  - Standard deviations
  - Coefficient of variation
  - Quartile analysis
  - Perspective scoring
- Generation of industry performance reports
- Visual representation of data distributions

## 🗄 Database Structure

The project uses SQLite database with a companies table containing:
- Company name
- INN
- OKVED codes (full and parts)
- Revenue
- Growth rate
- Owner information
