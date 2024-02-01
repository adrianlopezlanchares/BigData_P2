import os
import requests
from bs4 import BeautifulSoup
import yfinance as yf
import pandas as pd



def get_companies_cik(sector: str = "Consumer Discretionary") -> dict:   
    """Returns a dictionary with the symbols (tickers) and CIKs,
    of the companies in 'sector' (preset to consumer_discretionary)
    by webscraping the Wikipedia page of the S&P 500 companies.

    Args:
        sector: Sector of the companies to get the CIKs from.
    Returns:
        dict: Dictionary with the CIKs of the companies in 'tickers'
    """

    link = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    # Get the HTML code from the link
    html = requests.get(link).text

    # Create a BeautifulSoup object from the HTML
    soup = BeautifulSoup(html, "html.parser")

    # Get the table with the data
    table = soup.find("table", {"id": "constituents"})
    
    # Get the body of the table
    table_body = table.find("tbody")
    rows = table_body.find_all("tr")

    # Indexes of the columns we want
    symbol_index = 0
    GICS_sector_index = 2
    CIK_index = 6
    
    # Get the rows of the table
    rows = table_body.find_all("tr")

    # Create the dictionary with the symbols and CIKs
    companies_ciks_dict = {}

    # Select the rows that belong to the sector (the first row is the header)
    for row in rows[1:]:
        if row.find_all("td")[GICS_sector_index].text.strip() == sector:
            # Get the columns of each row
            cols = row.find_all("td")

            # Get the ticker
            symbol = cols[symbol_index].text.strip()

            # Get the CIK
            CIK = cols[CIK_index].text.strip()

            # Print the ticker and the CIK
            companies_ciks_dict[symbol] = CIK

    return companies_ciks_dict



def get_companies_dataframe(start_date: str, end_date: str,
        sector: str = "Consumer Discretionary", verbose: bool = False) -> pd.DataFrame:
    """Downloads data from yfinance for the selected time period for all
    companies in the S&P 500 index that belong to the given sector (preset
    to Consumer Discretionary).

    This function works as a generator, yielding each time the dataframe
    containing info about each stock. This is done in order to not load all
    the data into memory, so we only load data from each stock each time.
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
    
    Returns:
        generator: Generator that yields a dataframe for each stock"""
    
    year = start_date[:4]

    if verbose:
        print(f"\nDownloading data from {year}...")

    # Get the CIKs of the companies in the sector
    companies_ciks_dict = get_companies_cik(sector)

    for ticker, cik in companies_ciks_dict.items():
        # Download data from yfinance
        # Da error para ABNB en 2018 y 2019, no hay datos
        df = yf.download(ticker, start=start_date, end=end_date, progress=False)

        
        # Add ticker column to dataframe
        df["Ticker"] = ticker
        df["CIK"] = cik

        # Rename "Adj Close" column to "Adj_Close"
        df = df.rename(columns={"Adj Close": "Adj_Close"})

        # Change order: date, ticker, open, high, low, close, adj_close, volume
        df = df[["Ticker", "CIK", "Open", "High", "Low", "Close", "Adj_Close", "Volume"]]

        # Change the date from index to column
        df = df.reset_index()

        df.sort_values(by="Date")
        df["Date"] = df["Date"].astype(str)

        yield df


def control_data_directory(path: str) -> bool:
    """Given the path, the function checks if the data directory exists. If it doesn't, it creates it

    Args:
        path (str): Path to the data directory

    Returns:
        bool: True if the directory exists or was created, false if it wasn't
    """
    success = True

    if not os.path.isdir(path):
        try:
            os.mkdir(path)
        except OSError:
            print(f"Creation of the directory {path} failed")
            success = False
        else:
            print(f"Successfully created the directory {path}")

    return success