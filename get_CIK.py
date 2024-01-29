import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np


TICKERS = ['ABNB', 'AMZN', 'APTV', 'AZO', 'BBWI', 'BBY', 'BKNG', 'BWA',
           'CCL', 'CMG', 'CZR', 'DHI', 'DPZ', 'DRI', 'EBAY', 'ETSY', 'EXPE',
           'F', 'GM', 'GPC', 'GRMN', 'HAS', 'HD', 'HTL', 'KMX', 'LEN', 'LKQ',
           'LOW', 'LULU', 'LVS', 'MAR', 'MCD', 'MGM', 'MHK', 'NCLH', 'NKE',
           'NVR', 'ORLY', 'PHM', 'POOL', 'RCL', 'RL', 'ROST', 'SBUX', 'TJX',
           'TPR', 'TSCO', 'TSLA', 'ULTA', 'VFC', 'WHR', 'WYNN', 'YUM']



def get_CIK(tickers: list[str] = TICKERS) -> dict:   
    """Returns a dictionary with the CIKs of the companies in 'tickers', accesing to the Wikipedia page of the S&P 500 companies.
    Args:
        tickers (list[str]): List of tickers. Defaults to TICKERS.
    Returns:
        dict: Dictionary with the CIKs of the companies in 'tickers'
    """

    link = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    ciks_dict = {}

    # Get the HTML code from the link
    html = requests.get(link).text

    # Create a BeautifulSoup object from the HTML
    soup = BeautifulSoup(html, "html.parser")

    # Get the table with the data
    table = soup.find("table", {"class": "wikitable sortable"})
    table_body = table.find("tbody")

    # Get the rows of the table
    rows = table_body.find_all("tr")

    # eliminate the first row because it is the header
    rows = rows[1:]

    for row in rows:
        # Get the columns of each row
        cols = row.find_all("td")

        # Get the ticker
        ticker = cols[0].text.strip()

        # Get the CIK
        CIK = cols[6].text.strip()

        # Print the ticker and the CIK
        if ticker in tickers:
            ciks_dict[ticker] = CIK
    print(ciks_dict)
    return ciks_dict
            
if __name__ == "__main__":
    get_CIK()