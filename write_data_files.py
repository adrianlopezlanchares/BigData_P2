from fastavro import parse_schema, writer

# own imports
from data_download import *


def write_avro_file(start_date: str, end_date: str):
    """Downloads data from all stocks for the selected time period, and writes a .avro file containing the data.

    Args:
        start_date (string): Start date in YYY-MMM-DD format
        end_date (string): End date in YYY-MM-DD format
    """
    year = start_date[:4]

    year_data = get_companies_dataframe(start_date, end_date)


    schema = {
        "namespace": "ConsumerDiscretionary_2",
        "type": "record",
        "name": f"data_{start_date}_{end_date}",
        "fields": [
            {"name": "Date", "type": "string"},
            {"name": "Ticker", "type": "string"},
            {"name": "Open", "type": "float"},
            {"name": "High", "type": "float"},
            {"name": "Low", "type": "float"},
            {"name": "Close", "type": "float"},
            {"name": "Adj Close", "type": "float"},
            {"name": "Volume", "type": "float"},
        ]
    }
    parsed_schema = parse_schema(schema)

    records = year_data.to_dict("records")

    with open(f"data/stock_data_{year}", "wb") as out:
        writer(out, parsed_schema, records)

    return

def write_csv_file(start_date: str, end_date: str):
    """Downloads data from the selected year and saves it in a .csv file

    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
    """

    year = start_date[:4]
    df = get_companies_dataframe(start_date, end_date)
    df.to_csv(f"data/stock_data_{year}.csv", index=False)

    return

def write_json_file(start_date: str, end_date: str):
    """Downloads data from the selected year and saves it in a .JSON file

    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
    """

    year = start_date[:4]
    df = get_companies_dataframe(start_date, end_date)
    df.to_json(f"data/stock_data_{year}.json", orient="records")

    return

def write_xlsx_file(start_date: str, end_date: str):
    """Downloads data from the selected year and saves it in a .xlsx file

    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
    """

    year = start_date[:4]
    df = get_companies_dataframe(start_date, end_date)
    df.to_excel(f"data/stock_data_{year}.xlsx", index=False)

    return

def main():

    write_avro_file("2018-01-01", "2019-01-01")
    write_csv_file("2019-01-01", "2020-01-01")
    write_json_file("2020-01-01", "2021-01-01")
    write_xlsx_file("2021-01-01", "2022-01-01")

    return


if __name__ == "__main__":
    main()