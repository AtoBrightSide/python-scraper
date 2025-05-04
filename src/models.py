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
