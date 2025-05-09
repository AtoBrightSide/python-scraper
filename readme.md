# 13F Holdings Scraper

This project is a take-home assignment for gomanzanas. It asynchronously scrapes quarterly 13F-HR filings from [13f.info](https://13f.info), fetches detailed holdings data via the site's API, and aggregates the results using Pandas for analysis.

## Features

- **Asynchronous scraping** of fund managers and their 13F-HR filings.
- **API scraping - with exponential decay** to fetch detailed holdings for each filing.
- **Hierarchical batch** processing design to streamline scraping execution.
- **Data aggregation** and transformation using Pandas.
- **Transaction inference** (buy/sell/no change) and percentage change calculations.
- **Robust CSV** export of the final processed dataset
- **Improved logging** using Python's logging module for better tracking and error management.

## Prerequisites

- Python 3.7+
- [pip](https://pip.pypa.io/)

## Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/AtoBrightSide/scraping-assignment.git
   cd scraping-assignment/
   ```

2. **Create and activate a virtual environment:**

- Windows
  ```bash
  python -m venv env
  .\env\Scripts\activate
  ```
- Linux/macOS:
  ```bash
  python3 -m venv env
  source env/bin/activate
  ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. **Create `.env` file and set appropriate links for the following variables:**

   ```bash
   BASE_URL=https://13f.info/
   BASE_API_URL=https://13f.info/data/13f/
   ```

1. **Setting up directory for output file**

   - Create a directory called `data` at the root of the project

1. **Run the scraper:**

   ```bash
   python main.py
   ```

   - This will display a list of options to choose from on the terminal.
     - **Full Scrape** - scrapes all managers and their holdings (a - z), may take upto 2 hours.
     - **Merge Batches** - Merges all batch files into one final CSV file.
     - **Rescrape one batch** - Scrape holdings for managers starting with a specific letter (in case of errors).

1. **Output:**
   - **Final processed CSV** will be saved to `data/final_merged.csv` (~2.5GB)
   - **Batch CSVs** will be saved to `data/batches`.

## Project Structure

- **main.py**: Entry point for running the scraper.(will display a list of options)
- **src/scraper.py**: Main scraping and data processing logic ([`ThirteenFScraper`](src/scraper.py)).
- **src/api_client.py**: Handles API requests for holdings data ([`APIClient`](src/api_client.py)).
- **src/models.py**: Data models for managers and filings ([`Manager`](src/models.py), [`Filing`](src/models.py)).
- **utils.py**: Batch file merging functionality ([`merge_batch_files`](src/utils.py)).

## Notes

- The script only processes filings of type **13F-HR** and holdings with class **COM**.
- There are some filings (quarters) that have no holdings with **COM** class, those quarters have been logged on console.

## Scalability Thoughts

- For larger datasets, consider writing CSV files incrementally in batches rather than storing all batches(starting with letter X) in memory first before writing. Since currently most batches (~100MB) are manageable, batch writing hasn’t been prioritized, but it can improve memory efficiency for bigger workloads.
