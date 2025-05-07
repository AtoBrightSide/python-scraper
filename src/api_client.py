import aiohttp, asyncio, os


class APIClient:
    """
    Asynchronous client to fetch holdings data from the 13F API.
    """

    def __init__(self):
        try:
            self.base_api_url = os.environ["BASE_API_URL"]
        except KeyError as e:
            print(f"Environment variable {e} not found")
            raise e

    async def fetch_holdings(self, filing_id: str, session: aiohttp.ClientSession):
        """
        Fetch holdings data for a given quarter using asynchronously.
        Filters out records with None for stock_symbol or a non-null option_type. Only returns records with class 'COM'.
        Returns:
            List[dict]: [{ symbol, class, value, percentage, shares }, ... ]
        """
        max_retries = 3
        base_delay = 1  # initial delay (in seconds) for retry

        for attempt in range(max_retries + 1):
            try:
                url = self.base_api_url + filing_id
                async with session.get(url) as response:
                    if response.status == 500:
                        raise aiohttp.ClientResponseError(
                            status=response.status,
                            request_info=response.request_info,
                            history=response.history,
                            message=f"Server responded with status {response.status}",
                        )
                    response.raise_for_status()
                    data_json = await response.json()
                    holdings_list = data_json.get("data", [])
                    holdings = []
                    # [symbol, issuer_name, class, cusip, value, percentage, shares, principal, option_type]
                    for record in holdings_list:
                        # Skip records with missing symbols or that have a non-null option_type.
                        if record[0] is None or record[8] is not None:
                            continue

                        # Scrape holdings for COM class only
                        if record[2] == "COM":
                            holding = {
                                "symbol": record[0],
                                "class": record[2],
                                "value": record[4],
                                "percentage": record[5],
                                "shares": record[6],
                            }
                            holdings.append(holding)
                    return holdings

            except aiohttp.ClientResponseError as e:
                if e.status == 500:
                    if attempt < max_retries:
                        delay = base_delay * (2**attempt)
                        print(
                            f"Attempt {attempt + 1} for filing {filing_id} failed with status 500. Retrying in {delay} seconds..."
                        )
                        await asyncio.sleep(delay)
                        continue
                    else:
                        print(f"Max retries reached for filing {filing_id}.")
                        raise e
                else:
                    raise e
            except Exception as e:
                # For other types of exceptions, we'll similarly use the retry logic.
                if attempt < max_retries:
                    delay = base_delay * (2**attempt)
                    print(
                        f"Attempt {attempt + 1} for filing {filing_id} failed with error '{e}'. Retrying in {delay} seconds..."
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    print(
                        f"Max retries reached for filing {filing_id} after error '{e}'."
                    )
                    raise e
