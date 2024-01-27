import yfinance as yf
import pandas as pd
 
TICKERS = ['ABNB', 'AMZN', 'APTV', 'AZO', 'BBWI', 'BBY', 'BKNG', 'BWA',
           'CCL', 'CMG', 'CZR', 'DHI', 'DPZ', 'DRI', 'EBAY', 'ETSY', 'EXPE',
           'F', 'GM', 'GPC', 'GRMN', 'HAS', 'HD', 'HTL', 'KMX', 'LEN', 'LKQ',
           'LOW', 'LULU', 'LVS', 'MAR', 'MCD', 'MGM', 'MHK', 'NCLH', 'NKE',
           'NVR', 'ORLY', 'PHM', 'POOL', 'RCL', 'RL', 'ROST', 'SBUX', 'TJX',
           'TPR', 'TSCO', 'TSLA', 'ULTA', 'VFC', 'WHR', 'WYNN', 'YUM']
 

def create_dataframe(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    df = yf.download(ticker, start=start_date, end=end_date)
    # Add ticker column to dataframe
    df["Ticker"] = ticker

    # Change order: date, ticker, open, high, low, close, adj_close, volume
    df = df[["Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]

    # Change the date from index to column
    df = df.reset_index()
    return df


def join_dataframes(df_lst: list[pd.DataFrame]) -> pd.DataFrame:
    """Join and Order dataframes in df_lst by date and ticker"""
    df = pd.concat(df_lst)
    df = df.sort_values(by=["Date", "Ticker"])
    df = df.reset_index(drop=True)
    return df


def get_final_dataframe(start_date: str, end_date: str) -> pd.DataFrame:
    """Get dataframe for all companies (TICKERS) between
    start_date and end_date (YYYY-MM-DD)"""
    
    df_lst = []
    for ticker in TICKERS:
        df_lst.append(create_dataframe(ticker, start_date, end_date))
    
    
    return join_dataframes(df_lst)
