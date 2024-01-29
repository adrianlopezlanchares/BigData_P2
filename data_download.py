import yfinance as yf
import pandas as pd


TICKERS = ['ABNB', 'AMZN', 'APTV', 'AZO', 'BBWI', 'BBY', 'BKNG', 'BWA',
           'CCL', 'CMG', 'CZR', 'DHI', 'DPZ', 'DRI', 'EBAY', 'ETSY', 'EXPE',
           'F', 'GM', 'GPC', 'GRMN', 'HAS', 'HD', 'HTL', 'KMX', 'LEN', 'LKQ',
           'LOW', 'LULU', 'LVS', 'MAR', 'MCD', 'MGM', 'MHK', 'NCLH', 'NKE',
           'NVR', 'ORLY', 'PHM', 'POOL', 'RCL', 'RL', 'ROST', 'SBUX', 'TJX',
           'TPR', 'TSCO', 'TSLA', 'ULTA', 'VFC', 'WHR', 'WYNN', 'YUM']


def get_companies_dataframe(start_date: str, end_date: str, 
                            tickers: list[str] = TICKERS, verbose: bool = False) -> pd.DataFrame:
    """Downloads data from yfinance for the selected time period for
    all companies in 'tickers' and returns a dataframe with the data.
    'ticker' defaults to all companies in the Consumer Discretionary
    sector in the S&P 500.

    This function works as a generator, yielding each time the dataframe containing info about each stock.
    This is done in order to not load all the data into memory, so we only load data from each stock each time.
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
    
    Returns:
        pd.DataFrame: Dataframe containing data from all companies in 'tickers'"""
    
    year = start_date[:4]

    if verbose:
        print(f"\nDownloading data from {year}...")

    for ticker in tickers:
        # Download data from yfinance
        # Da error para ABNB en 2018 y 2019, no hay datos
        df = yf.download(ticker, start=start_date, end=end_date, progress=False)

        
        # Add ticker column to dataframe
        df["Ticker"] = ticker

        # Change order: date, ticker, open, high, low, close, adj_close, volume
        df = df[["Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]

        # Change the date from index to column
        df = df.reset_index()

        df.sort_values(by="Date")
        df["Date"] = df["Date"].astype(str)

        yield df
