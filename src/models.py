class Manager:
    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url
        self.filings = []  # Will hold Filing objects.


class Filing:
    def __init__(self, quarter: str, filing_date: str, filing_id: str):
        self.quarter = quarter
        self.filing_date = filing_date
        self.filing_id = filing_id


class Holding:
    def __init__(self, symbol: str, cl: str, value: str, percentage: str, shares: str):
        self.symbol = symbol
        self.cl = cl
        self.value = value
        self.percentage = percentage
        self.shares = shares


class Record:
    def __init__(
        self, fund_name, filing_date, quarter, stock_symbol, cl, value_000, shares
    ):
        self.fund_name = fund_name
        self.filing_date = filing_date
        self.quarter = quarter
        self.stock_symbol = stock_symbol
        self.cl = cl
        self.value_000 = value_000
        self.shares = shares
