import yfinance as yf
import pandas as pd

TICKERS = []


def download_stock_data(ticker, start_date, end_date):

    data = yf.download(ticker, start=start_date, end=end_date)

    return data


def write_avro_file(start_date, end_date):
    """Downloads data from the selected year (end_date - start_date) and writes it
       into a .avro file

    Args:
        start_date (string): Start date in YYYY-MM-DD format
        end_date (string): End date in YYYY-MM-DD format
    """

    avro_dataframe = pd.DataFrame('Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume')

    for ticker in TICKERS:
        data = download_stock_data(ticker, start_date, end_date)
        data['Ticker'] = ticker

        avro_dataframe.append(data)



if __name__ == "__main__":
    download_stock_data('APTV', '2018-01-01', '2024-01-01')