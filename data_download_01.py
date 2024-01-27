import yfinance as yf
import pandas as pd

TICKERS = ["ABNB", "AMZN", "APTV", "AZO", "BBWI", "BBY"]
# CIFS = []


def download_stock_data(ticker, start_date, end_date):
    """Downloads data from selected time period and stock

    Args:
        ticker (string): Ticker symbol for the selected stock
        start_date (string): Start date in YYYY-MM-DD format
        end_date (_type_): End date in YYYY-MM-DD format

    Returns:
        pandas.DataFrame: Dataframe containing downloaded data
    """

    data = yf.download(ticker, start=start_date, end=end_date)

    return data


def download_year_data(start_date, end_date):
    """Downloads data from the selected year (end_date - start_date) and writes it
       into a .avro file

    Args:
        start_date (string): Start date in YYYY-MM-DD format
        end_date (string): End date in YYYY-MM-DD format

    Returns:
        pandas.DataFrame: Dataframe containing data from all stocks in the sector, for the selected year
    """

    columns = ['Ticker', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']

    # Falta poner columna CIF
    
    stock_data = {ticker: None for ticker in TICKERS}

    for i, ticker in enumerate(TICKERS):
        data = download_stock_data(ticker, start_date, end_date)
        data['Ticker'] = ticker
        # data['CIF'] = CIFS[i]

        stock_data[ticker] = data

        stock_data[ticker] = stock_data[ticker].reset_index().rename(columns={"index": "Date"})
        stock_data[ticker]['Date'] = pd.to_datetime(stock_data[ticker]['Date'])
        stock_data[ticker].sort_values(by=["Date"])
        stock_data[ticker]['Date'] = stock_data[ticker]['Date'].astype(str)


    # Melt each DataFrame to have 'Date', 'Ticker', and other columns
    melted_dfs = [pd.melt(stock_data[ticker], 
                          id_vars=['Date', 'Ticker'], 
                          value_vars=['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'], 
                          var_name='Variable', 
                          value_name='Value') for ticker in stock_data]

    # Concatenate the melted DataFrames
    year_data = pd.concat(melted_dfs, ignore_index=True)

    # Sort the DataFrame by 'Date' and 'Ticker'
    year_data = year_data.sort_values(by=['Date', 'Ticker'])

    # Pivot the DataFrame to have 'Variable' as columns
    year_data = year_data.pivot(index=['Date', 'Ticker'], columns='Variable', values='Value').reset_index()

    # Reorder the columns
    year_data = year_data[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
    

    return year_data