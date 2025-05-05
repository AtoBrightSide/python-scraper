import aiohttp


class APIClient:
    """
    Asynchronous client to fetch holdings data from the 13F API.
    """

    def __init__(self):
        self.base_api_url = "https://13f.info/data/13f/"

    async def fetch_holdings(self, filing_id: str, session: aiohttp.ClientSession):
        """
        Fetch holdings data for a given quarter using asynchronously.
        Filters out records with None for stock_symbol or a non-null option_type. Only returns records with class 'COM'.
        Returns:
            List[dict]: [{ symbol, class, value, percentage, shares }, ... ]
        """
        url = self.base_api_url + filing_id
        async with session.get(url) as response:
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
