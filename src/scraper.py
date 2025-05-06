import re, time, os
import asyncio
import aiohttp
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

# Local imports
from src.models import Manager, Filing
from src.api_client import APIClient


class ThirteenFScraper:
    def __init__(self, output_filename="./data/final.csv"):
        self.base_url = "https://13f.info/"
        self.managers_url = f"{self.base_url}/managers"
        self.api_client = APIClient()
        
        self.output_filename = os.path.join("data", output_filename)
        os.makedirs("data", exist_ok = True)

    async def get_managers(self, session: aiohttp.ClientSession):
        """
        Scrape the /managers page and return a list of Manager objects.
        """
        print("Loading managers ...")
        async with session.get(self.managers_url) as response:
            response.raise_for_status()
            text = await response.text()
            soup = BeautifulSoup(text, "html.parser")
            managers = []

            table = soup.find(
                "table", class_=lambda value: value and "table-fixed" in value
            )
            if not table:
                print("Manager table not found on the page.")
                return managers

            tbody = table.find("tbody")
            for tr in tbody.find_all("tr"):
                cells = tr.find_all("td")
                if len(cells) >= 3:
                    name_cell = cells[0]
                    a_tag = name_cell.find("a")
                    manager_name = (
                        a_tag.get_text(strip=True)
                        if a_tag
                        else name_cell.get_text(strip=True)
                    )
                    manager_url = (
                        self.base_url.rstrip("/") + a_tag["href"]
                        if (a_tag and a_tag.get("href"))
                        else None
                    )
                    managers.append(Manager(manager_name, manager_url))

            print(f"Finished loading {len(managers)} managers")
            return managers

    async def get_filings_for_manager(
        self, manager: Manager, session: aiohttp.ClientSession
    ):
        """
        Asynchronously fetch a manager's page, parse the filings table, and populate the manager.filings list.
        Only filings with form type "13F-HR" are considered.
        """
        async with session.get(manager.url) as response:
            response.raise_for_status()
            text = await response.text()
            soup = BeautifulSoup(text, "html.parser")
            filings = []

            table = soup.find("table", id="managerFilings")
            if not table:
                print(f"Filings table not found on manager page: {manager.url}")
                return

            tbody = table.find("tbody")
            if not tbody:
                print("Filings table body not found.")
                return

            for tr in tbody.find_all("tr"):
                cells = tr.find_all("td")
                if len(cells) < 7:
                    continue
                form_type = cells[4].get_text(strip=True)
                if form_type != "13F-HR":
                    continue
                quarter = cells[0].find("a").get_text(strip=True)
                filing_date = cells[5].get_text(strip=True)
                filing_id = cells[6].get_text(strip=True)
                filings.append(Filing(quarter, filing_date, filing_id))

            filings.reverse()  # oldest first
            manager.filings = filings

    async def fetch_all_holdings(
        self, manager: Manager, session: aiohttp.ClientSession
    ):
        """
        For a given manager, fetch holdings for all quarters concurrently.
        Returns a tuple (holdings_by_quarter, failed_records) where:
          - holdings_by_quarter: a dictionary mapping Filing objects to their holdings lists.
          - failed_records: a list of dictionaries, each corresponding to a quarter whose holdings failed to be fetched.
        """
        tasks = []
        for filing in manager.filings:
            tasks.append(self.api_client.fetch_holdings(filing.filing_id, session))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        holdings_by_quarter = {}
        failed_records = []
        for filing, res in zip(manager.filings, results):
            if isinstance(res, Exception):
                print(f"Error fetching holdings for {manager.name} quarter: {filing.quarter} \n{res}")
                failed_records.append(
                    {
                        "fund_name": manager.name,
                        "filing_id": filing.filing_id,
                        "quarter": filing.quarter,
                        "filing_date": filing.filing_date,
                        "error": str(res),
                    }
                )
            else:
                holdings_by_quarter[filing] = res

        print(
            f"Finished scraping {len(holdings_by_quarter.keys())} holdings for {manager.name}"
        )
        return holdings_by_quarter, failed_records

    def process_records(self, records):
        """
        Processes raw records using Pandas:
          - Converts to DataFrame
          - Sorts and groups the data
          - Computes the previous_shares, change, percentage_change
          - Infers the transaction_type
          - Writes the final CSV file (excluding the temporary columns)
        """
        # Convert aggregated records into a DataFrame.
        df = pd.DataFrame(records)
        df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
        df = df.sort_values(by=["fund_name", "stock_symbol", "filing_date"])

        # creating prev_shares column for calculation purposes
        df["prev_shares"] = df.groupby(["fund_name", "stock_symbol"])["shares"].shift(1)

        # if there is no prev_share, then the current holding is NEW
        df["new_holding"] = df["prev_shares"].isna()

        # Calculate the share change, based on whether the holding is new or not
        df["change"] = df.apply(
            lambda row: (
                row["shares"]
                if row["new_holding"]
                else row["shares"] - row["prev_shares"]
            ),
            axis=1,
        )

        # if holding is new, the pct_change will be NaN, since there would be no meaning in calculating the change
        df["pct_change"] = df.apply(
            lambda row: (
                np.nan
                if row["new_holding"] or row["prev_shares"] == 0
                else round((row["change"] / row["prev_shares"]) * 100, 2)
            ),
            axis=1,
        )

        # if holding is new, its considered a buy.
        df["inferred_transaction_type"] = df.apply(
            lambda row: (
                "buy"
                if row["new_holding"] and row["shares"] > 0
                else (
                    "buy"
                    if (not row["new_holding"] and row["change"] > 0)
                    else (
                        "sell"
                        if (not row["new_holding"] and row["change"] < 0)
                        else "no change"
                    )
                )
            ),
            axis=1,
        )

        # Remove temporary columns that are not required for final output
        df.drop(columns=["prev_shares", "new_holding"], inplace=True)

        df.to_csv(self.output_filename, index=False)
        print(f"Final CSV saved to {self.output_filename}")

    async def run(self):
        """
        Main pipeline:
          1. Retrieve manager data concurrently.
          2. For each manager, concurrently fetch filings, then fetch API holdings data.
          3. Accumulate all records.
          4. Use Pandas for calculations (groupby/shift) to infer transaction type.
          5. Write the final CSV and log failed records(if there are any).
        """

        failed_records_total = []
        async with aiohttp.ClientSession(
            headers={"User-Agent": "Mozilla/5.0 (compatible; DataScraper/1.0)"}
        ) as session:
            start_time = time.time()
            managers = await self.get_managers(session)
            # Fetch filings in parallel for all managers.
            get_filings_tasks = []
            get_filings_tasks = [
                self.get_filings_for_manager(manager, session) for manager in managers
            ]
            await asyncio.gather(*get_filings_tasks)
            records = []

            # Process each manager sequentially
            for manager in managers:
                # if a manager has no quarters of form type 13F-HR, we skip it.
                if not manager.filings:
                    continue

                print(f"Processing manager: {manager.name} | URL: {manager.url}")
                holdings_by_quarter, failed_records = await self.fetch_all_holdings(
                    manager, session
                )
                failed_records_total.extend(failed_records)
                for filing in manager.filings:
                    holdings = holdings_by_quarter.get(filing)
                    if not holdings:
                        print(
                            f"Couldn't find holdings for manager: {manager.name}, quarter: {filing.quarter}"
                        )
                        continue
                    for holding in holdings:
                        symbol = holding["symbol"]
                        record = {
                            "fund_name": manager.name,
                            "filing_date": filing.filing_date,
                            "quarter": filing.quarter,
                            "stock_symbol": symbol,
                            "cl": holding["class"],
                            "value_($000)": holding["value"],
                            "shares": holding["shares"],
                        }
                        records.append(record)

            if not records:
                print("No records fetched. Exiting...")
                return

            print(f"# # # Finished processing. # # #")
            end_time = time.time()

            print(f"Time for fetching data: {end_time - start_time}")

            self.process_records(records)

            print(f"Time taken to write to file: {time.time() - end_time}")

            print(f"Total failed holdings: {len(failed_records_total)}")
            if failed_records_total:
                print("Failed records:")
                for rec in failed_records_total:
                    print(
                        f"Manager: {rec['fund_name']}, Quarter: {rec['quarter']}, Error: {rec['error']}"
                    )
