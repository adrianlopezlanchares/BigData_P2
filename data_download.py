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
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
    
    Returns:
        pd.DataFrame: Dataframe containing data from all companies in 'tickers'"""
    
    year = start_date[:4]

    if verbose:
        print(f"\nDownloading data from {year}...")

    df_lst = []
    for ticker in tickers:
        # Download data from yfinance
        df = yf.download(ticker, start=start_date, end=end_date, progress=False)
        
        # Add ticker column to dataframe
        df["Ticker"] = ticker

        # Change order: date, ticker, open, high, low, close, adj_close, volume
        df = df[["Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]

        # Change the date from index to column
        df = df.reset_index()

        df_lst.append(df)

    df_companies = pd.concat([df for df in df_lst if not df.empty])

    df_companies["Date"] = pd.to_datetime(df_companies["Date"])
    df_companies = df_companies.sort_values(by=["Date", "Ticker"])
    df_companies = df_companies.reset_index(drop=True)
    df_companies["Date"] = df_companies["Date"].dt.strftime("%Y-%m-%d")

    if verbose:
        print("Done.\n")

    return df_companies
