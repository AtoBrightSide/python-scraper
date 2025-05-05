# 13F Holdings Scraper

This is a submission for the test assignment from gomanzanas. It scrapes quarterly 13F-HR filings from [13f.info](https://13f.info) asynchronously, fetches detailed holdings data from the API, and aggregates the data using Pandas for further analysis.

## Prerequisites

- Python 3.7+
- [pip](https://pip.pypa.io/)

## Installation

1. Clone this repository:

   ```bash
   git clone https://github.com/AtoBrightSide/scraper-assignment.git
   ```

2. Navigate to the directory:

   ```bash
   cd scraper-assignment/
   ```

3. Activate virtual environment 
   
   For Windows
   ```bash
      python -m venv <name-of-your-env>
      source <name-of-your-env>\Scripts\activate
   ```
   For Linux
   ```bash
      python3 -m venv <name-of-your-env>


3. Install the requirements

   ```bash
   pip install -r requirements.txt
   ```

4. Run the server
   ```bash
   python main.py
   ```
