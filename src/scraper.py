import re, time, os, csv, string, requests
import asyncio, logging
import aiohttp
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from collections import defaultdict

from src.models import Manager, Filing
from src.api_client import APIClient
from src.utils import merge_batch_files

logger = logging.getLogger(__name__)


class ThirteenFScraper:
    def __init__(self, output_filename="./data/final.csv"):
        try:
            # load from environment variable
            self.base_url = os.environ["BASE_URL"]
            self.managers_url = f"{self.base_url}/managers/"
            self.api_client = APIClient()
        except KeyError as e:
            print(f"Environment variable {e} not found")
            raise e

        self.output_filename = os.path.join("data", output_filename)
        os.makedirs("data", exist_ok=True)

    async def get_managers_by_letter(
        self, manager_letter_url, session: aiohttp.ClientSession
    ):
        managers = []
        async with session.get(manager_letter_url) as response:
            try:
                response.raise_for_status()
            except Exception as e:
                print(f"Failed to fetch {manager_letter_url} with error: {e}")
                return []

            text = await response.text()
            soup = BeautifulSoup(text, "html.parser")

            table = soup.find(
                "table", class_=lambda value: value and "table-fixed" in value
            )

            if not table:
                print(f"Manager table not found on the page: {manager_letter_url}")
                return []

            tbody = table.find("tbody")
            if not tbody:
                print(f"Manager table body not found on page: {manager_letter_url}")
                return []

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
                        requests.compat.urljoin(self.base_url, a_tag["href"])
                        if a_tag and a_tag.get("href")
                        else None
                    )

                    managers.append(Manager(manager_name, manager_url))

            return managers

    async def get_managers(self, session: aiohttp.ClientSession):
        """
        Scrapes all managers from a - z and returns a list of Manager objects
        """
        tasks = []
        for letter in string.ascii_lowercase:
            logger.info(f"Loading managers that start with {letter.capitalize()} ...")
            manager_letter_url = self.managers_url + letter
            tasks.append(self.get_managers_by_letter(manager_letter_url, session))

        results = await asyncio.gather(*tasks)
        managers = []
        for manager_list in results:
            managers.extend(manager_list)

        logger.info(f"Finished loading {len(managers)} managers")
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
                logger.warning(
                    f"Filings table not found on manager page: {manager.url}"
                )
                return

            tbody = table.find("tbody")
            if not tbody:
                logger.warning("Filings table body not found.")
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
                logger.error(
                    f"Error fetching holdings for {manager.name} quarter: {filing.quarter} \n{res}"
                )
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

        logger.info(
            f"Finished scraping {len(holdings_by_quarter.keys())} holdings for {manager.name}"
        )
        return holdings_by_quarter, failed_records

    def process_records(self, records, output_filename=None):
        """
        Processes raw records using Pandas:
          - Converts to DataFrame
          - Sorts and groups the data
          - Computes the previous_shares, change, percentage_change
          - Infers the transaction_type
          - Writes the final CSV file (excluding the temporary columns)
        """
        df = pd.DataFrame(records)
        df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
        df = df.sort_values(
            by=["fund_name", "stock_symbol", "filing_date"],
            key=lambda col: col.str.lower() if col.dtype == "object" else col,
        )

        # creating prev_shares column for calculation purposes
        df["prev_shares"] = df.groupby(["fund_name", "stock_symbol"])["shares"].shift(1)

        # if there is no prev_share, then the current holding is NEW
        df["new_holding"] = df["prev_shares"].isna()

        # calculate the share change, based on whether the holding is new or not
        df["change"] = np.where(
            df["new_holding"], df["shares"], df["shares"] - df["prev_shares"]
        )

        # compute percentage change only for rows where prev_shares is non-zero and not a new holding
        # if holding is new, the pct_change will be NaN, since there would be no meaning in calculating the change
        pct_condition = (~df["new_holding"]) & (df["prev_shares"] != 0)
        df["pct_change"] = np.nan
        df.loc[pct_condition, "pct_change"] = np.round(
            (df.loc[pct_condition, "change"] / df.loc[pct_condition, "prev_shares"])
            * 100,
            2,
        )

        # if holding is new, its considered a buy.
        df["inferred_transaction_type"] = df.apply(
            lambda row: (
                "new_buy"
                if row["new_holding"] and row["shares"] > 0
                else (
                    "full_sell"
                    if (
                        not row["new_holding"]
                        and row["shares"] == 0
                        and row["prev_shares"] > 0
                    )
                    else (
                        "buy"
                        if (not row["new_holding"] and row["change"] > 0)
                        else (
                            "sell"
                            if (not row["new_holding"] and row["change"] < 0)
                            else "no_change"
                        )
                    )
                )
            ),
            axis=1,
        )

        # remove temporary columns that are not required for final output
        df.drop(columns=["prev_shares", "new_holding"], inplace=True)

        df.to_csv(
            output_filename if output_filename else self.output_filename, index=False
        )

    async def _process_manager_batch(
        self, letter: str, managers_list: list, session: aiohttp.ClientSession
    ):
        """
        Common helper that processes a batch of managers for a given letter.
        Steps:
            1. Fetch filings concurrently.
            2. For managers with filings, fetch holdings concurrently.
            3. Accumulate records.
            4. Process and write CSV file for the batch.
            5. Return number of records processed and list of failed records.
        """
        logger.info(
            f"Starting batch for letter {letter} with {len(managers_list)} managers"
        )

        # fetch filings concurrently for all managers in this batch.
        tasks_filings = [
            self.get_filings_for_manager(m, session) for m in managers_list
        ]
        await asyncio.gather(*tasks_filings)

        # remove managers without filings.
        managers_with_filings = [m for m in managers_list if m.filings]
        batch_records = []
        batch_failed = []

        if managers_with_filings:
            # get holdings for all managers concurrently
            holdings_tasks = [
                self.fetch_all_holdings(m, session) for m in managers_with_filings
            ]
            holdings_results = await asyncio.gather(
                *holdings_tasks, return_exceptions=True
            )

            # process the results for each manager
            for manager, result in zip(managers_with_filings, holdings_results):
                if isinstance(result, Exception):
                    logger.error(
                        f"Error fetching holdings for {manager.name} at {manager}: {result}"
                    )
                    batch_failed.append(
                        {
                            "fund_name": manager.name,
                            "filing_id": "",
                            "quarter": "all",
                            "filing_date": "",
                            "error": str(result),
                        }
                    )
                else:
                    holdings_by_quarter, failed_records = result
                    batch_failed.extend(failed_records)

                    # process this manager's filings
                    for filing in manager.filings:
                        holdings = holdings_by_quarter.get(filing)

                        # if this quarter doesn't have classes of 'COM'
                        if not holdings:
                            logger.warning(
                                f"Couldn't find holdings for manager: {manager.name}, quarter: {filing.quarter}"
                            )
                            continue

                        # accumulate each holding record
                        for holding in holdings:
                            record = {
                                "fund_name": manager.name,
                                "filing_date": filing.filing_date,
                                "quarter": filing.quarter,
                                "stock_symbol": holding["symbol"],
                                "cl": holding["class"],
                                "value_($000)": holding["value"],
                                "shares": holding["shares"],
                            }
                            batch_records.append(record)
        else:
            logger.info(f"No managers with filings for letter: {letter}")

        if batch_records:
            # ensure batch directory exists.
            batch_dir = os.path.join("data", "batches")
            os.makedirs(batch_dir, exist_ok=True)
            batch_filename = os.path.join(batch_dir, f"final_{letter}.csv")
            self.process_records(batch_records, output_filename=batch_filename)
            logger.info(f"Written {len(batch_records)} records to {batch_filename}")
        else:
            logger.warning(f"No records found for letter: {letter}.")

        return len(batch_records), batch_failed

    async def run_batch(self, letter):
        """
        Processes managers for by a specific letter:
        1. Retrieve manager data for the specified letter.
        2. For each manager, concurrently fetch filings and then API holdings.
        3. Accumulate records for this batch.
        4. Use Pandas to process the records and infer transaction types.
        5. Write the processed records to a batch CSV file.
        6. Log any failed holdings.
        """
        logger.info(f"Starting batch run for letter: {letter.upper()}")
        failed_records_total = []
        async with aiohttp.ClientSession(
            headers={"User-Agent": "Mozilla/5.0 (compatible; DataScraper/1.0)"}
        ) as session:
            start_time = time.time()

            # retrieve managers only for the specified letter.
            managers_list = await self.get_managers_by_letter(
                self.managers_url + letter, session
            )
            if not managers_list:
                logger.warning(f"No managers found for letter '{letter}'.")
                return

            await self._process_manager_batch(letter.upper(), managers_list, session)

            end_time = time.time()

            logger.info(
                f"Batch run for letter {letter.upper()} completed in {round((end_time - start_time) / 60)} minutes"
            )

    async def run(self):
        """
        Main pipeline:
          1. Retrieve all managers data concurrently.
          2. For each manager, concurrently fetch filings, then fetch API holdings data.
          3. Accumulate all records.
          4. Use Pandas for calculations (groupby/shift) to infer transaction type.
          5. Write the final CSV and log failed records(if there are any).
        """

        logger.info("Starting full scraping pipeline...")
        failed_records_total = []
        total_records = 0
        async with aiohttp.ClientSession(
            headers={"User-Agent": "Mozilla/5.0 (compatible; DataScraper/1.0)"}
        ) as session:
            batch_start_time = time.time()

            managers = await self.get_managers(session)

            # create a map of letter: List [Managers]
            managers_by_letter = defaultdict(list)
            for manager in managers:
                initial = manager.name[0].upper()
                managers_by_letter[initial].append(manager)

            # Process managers by letter
            for letter, managers_list in managers_by_letter.items():
                logger.info(
                    f"\n=== Processing batch for letter: {letter} with {len(managers_list)} managers ==="
                )

                records_count, batch_failed = await self._process_manager_batch(
                    letter, managers_list, session
                )
                total_records += records_count
                failed_records_total.extend(batch_failed)

            if total_records == 0:
                logger.warning("No records fetched. Exiting...")
                return

            total_time = time.time() - batch_start_time
            logger.info(
                f"\nFinished scraping. Total records across all batches: {total_records}"
            )
            logger.info(
                f"Total time taken for fetching data: {round(total_time / 60, 2)} minutes\n"
            )

            logger.info(f"Total failed holdings: {len(failed_records_total)}")
            if failed_records_total:
                failed_file = os.path.join("data", "failed_holdings.csv")
                with open(failed_file, "w", newline="") as errorFile:
                    writer = csv.writer(errorFile)
                    writer.writerow(["fund_name", "quarter", "error"])
                    logger.info("Failed records:")
                    for rec in failed_records_total:
                        logger.error(
                            f"Manager: {rec['fund_name']}, Quarter: {rec['quarter']}, Error: {rec['error']}"
                        )
                        writer.writerow(
                            [rec["fund_name"], rec["quarter"], rec["error"]]
                        )
                logger.info(f"Failed holdings logged to {failed_file}")
