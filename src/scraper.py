import re
import csv
from collections import defaultdict
import requests
from bs4 import BeautifulSoup

from src.models import Manager, Filing
from src.api_client import APIClient


class ThirteenFScraper:
    def __init__(self):
        self.base_url = "https://13f.info/"
        self.managers_url = f"{self.base_url}/managers"
        self.session = requests.Session()
        self.session.headers.update(
            {'User-Agent': 'Mozilla/5.0 (compatible; DataScraper/1.0)'})
        self.api_client = APIClient()

    def sanitize_filename(self, name: str) -> str:
        # Remove surrounding whitespace and quotes.
        name = name.strip().strip('"')
        # Replace ampersands with 'and'.
        name = name.replace('&', 'and')
        # Remove commas and periods, then replace spaces/slashes with underscores.
        name = name.replace(',', '').replace('.', '')
        name = re.sub(r'[ /]+', '_', name)
        # Remove any characters not suitable for filenames and convert to lowercase.
        return re.sub(r'[^\w\-]', '', name).lower()

    def get_managers(self):
        """
        Scrapes the /managers page and returns a list of Manager objects.
        """
        response = self.session.get(self.managers_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        managers = []

        # Find the table with a class that contains "table-fixed"
        table = soup.find(
            "table", class_=lambda value: value and "table-fixed" in value)
        if not table:
            print("Manager table not found on the page.")
            return managers

        tbody = table.find("tbody")
        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) >= 3:
                name_cell = cells[0]
                a_tag = name_cell.find("a")
                manager_name = a_tag.get_text(
                    strip=True) if a_tag else name_cell.get_text(strip=True)
                manager_url = (requests.compat.urljoin(self.base_url, a_tag['href'])
                               if (a_tag and a_tag.get('href')) else None)
                managers.append(Manager(manager_name, manager_url))
        return managers

    def get_filings_for_manager(self, manager: Manager):
        """
        For a given Manager, fetch the filings page and populate its filings list.
        Only filings with form type "13F-HR" are considered.
        """
        response = self.session.get(manager.url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        filings = []

        table = soup.find("table", id="managerFilings")
        if not table:
            print(
                f"Filings table not found on the manager page: {manager.url}")
            return

        tbody = table.find("tbody")
        if not tbody:
            print("Filings table body not found.")
            return

        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")
            # Expected columns:
            # 0 - Quarter, 1 - Holdings, 2 - Value, 3 - Top Holdings,
            # 4 - Form Type, 5 - Filing Date, 6 - Filing ID
            if len(cells) < 7:
                continue

            form_type = cells[4].get_text(strip=True)
            if form_type != "13F-HR":
                continue

            quarter = cells[0].find("a").get_text(strip=True)
            filing_date = cells[5].get_text(strip=True)
            filing_id = cells[6].get_text(strip=True)
            filings.append(Filing(quarter, filing_date, filing_id))

        filings.reverse()  # Chronological order: oldest first.
        manager.filings = filings

    def run(self, output_filename='./data/final.csv'):
        """
        Main pipeline:
          1. Get all the managers.
          2. For each manager, get all quarterly filings of the type 13F-HR.
          3. For each quarter, fetch holdings via the API.
          4. Infer transaction type based on the change in shares between consecutive years.
          5. Write data out to a CSV.
        """
        managers = self.get_managers()
        with open(output_filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["manager_name", "date", "quarter", "symbol", "class", "value",
                             "current_shares", "change", "pct_change", "action"])

            for manager in managers:
                print(
                    f"Processing manager: {manager.name} | URL: {manager.url}")
                self.get_filings_for_manager(manager)

                # Tracker for the previous filing's holdings for each symbol.
                prev_holdings = {}
                for filing in manager.filings:
                    try:
                        holdings = self.api_client.fetch_holdings(
                            filing.filing_id)
                        for holding in holdings:
                            symbol = holding["symbol"]
                            current_shares = holding["shares"]

                            if symbol in prev_holdings:
                                change = current_shares - prev_holdings[symbol]
                                pct_change = (round((change / prev_holdings[symbol]) * 100, 2)
                                              if prev_holdings[symbol] != 0 else 0)
                            else:
                                change = 0
                                pct_change = 0

                            action = "buy" if change > 0 else (
                                "sell" if change < 0 else "same")
                            # Update tracker per symbol.
                            prev_holdings[symbol] = current_shares

                            row = [manager.name, filing.filing_date, filing.quarter, symbol,
                                   holding["class"], holding["value"], current_shares,
                                   change, pct_change, action]
                            writer.writerow(row)
                            print(f"Added row: {row}")
                    except requests.HTTPError as e:
                        print(f"HTTP error for filing {filing.filing_id}: {e}")
                    except Exception as e:
                        print(f"Error for filing {filing.filing_id}: {e}")
