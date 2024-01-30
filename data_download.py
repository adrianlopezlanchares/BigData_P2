import yfinance as yf
import pandas as pd

# own imports
from get_companies import get_companies_cik


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
