# 13F Holdings Scraper

This project is a take-home assignment for gomanzanas. It asynchronously scrapes quarterly 13F-HR filings from [13f.info](https://13f.info), fetches detailed holdings data via the site's API, and aggregates the results using Pandas for analysis.

## Features

- **Asynchronous scraping** of fund managers and their 13F-HR filings.
- **API scraping - with exponential decay** to fetch detailed holdings for each filing.
- **Data aggregation** and transformation using Pandas.
- **Transaction inference** (buy/sell/no change) and percentage change calculations.
- **CSV export** of the final processed dataset.
- **Error logging** for failed records.

## Prerequisites

- Python 3.7+
- [pip](https://pip.pypa.io/)

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/AtoBrightSide/scraper-assignment.git
   cd scraper-assignment/
   ```

2. **Create and activate a virtual environment:**

   - **Windows:**
     ```bash
     python -m venv env
     .\env\Scripts\activate
     ```
   - **Linux/macOS:**
     ```bash
     python3 -m venv env
     source env/bin/activate
     ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. **Run the scraper:**
   ```bash
   python main.py
   ```

2. **Setting up directory for output file**
      
      - Create a directory called `data` at the root of the project

      - When prompted, enter a name for the output file (the CSV will be saved in the `data/` directory).

3. **Output:**
   - The processed CSV will be saved to `data/<your_output_file>.csv`.

## Project Structure
- **main.py**: Entry point for running the scraper.
- **src/scraper.py**: Main scraping and data processing logic ([`ThirteenFScraper`](src/scraper.py)).
- **src/api_client.py**: Handles API requests for holdings data ([`APIClient`](src/api_client.py)).
- **src/models.py**: Data models for managers and filings ([`Manager`](src/models.py), [`Filing`](src/models.py)).

## Notes

- The script only processes filings of type **13F-HR** and holdings with class **COM**.
- For large data sets, the process may take several minutes (as of right now, it takes ~8 minutes, which was brought down from ~60 minutes by incorporating concurrent requests)

## Additional Thoughts

- For larger data sets, it will be preferable to write the data to the csv in batches, instead of writing all rows at once (as has been done here), but since the size of the data is small (~100MB), storing the records in memory then writing it to the csv sounded like the more optimal approach.