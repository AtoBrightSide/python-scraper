import requests


class APIClient:
    """
    Responsible for fetching holding data from the 13F API.
    """

    def __init__(self):
        self.base_api_url = "https://13f.info/data/13f/"

    def fetch_holdings(self, filing_id: str):
        url = self.base_api_url + filing_id
        response = requests.get(url)
        response.raise_for_status()

        data_json = response.json()
        holdings_list = data_json.get("data", [])
        holdings = []
        # Expected record: [symbol, issuer_name, class, cusip, value, percentage, shares, principal, option_type]
        for record in holdings_list:
            # Filter only COM class holdings with no option_type.
            if record[0] is None or record[8] is not None:
                continue

            if record[2] == 'COM':
                holding = {
                    "symbol": record[0],
                    "class": record[2],
                    "value": record[4],
                    "percentage": record[5],
                    "shares": record[6]
                }
                holdings.append(holding)
        return holdings
