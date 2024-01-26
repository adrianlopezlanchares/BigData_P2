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

    # Falta poner columna CIF
    year_data = pd.DataFrame(columns=['Ticker', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])

    for i, ticker in enumerate(TICKERS):
        data = download_stock_data(ticker, start_date, end_date)
        data['Ticker'] = ticker
        # data['CIF'] = CIFS[i]

        year_data = pd.concat([year_data, data])

    year_data = year_data.reset_index().rename(columns={"index": "Date"})

    year_data['Date'] = pd.to_datetime(year_data['Date'])

    year_data.sort_values(by=["Date"]) # NO LO HACE BIEN: FECHAS REPETIDAS DEBERIAN IR SEGUIDAS

    return year_data