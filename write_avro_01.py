from fastavro import parse_schema, reader, writer

# own imports
from data_download_01 import *


def write_avro_file(start_date, end_date):
    """Downloads data from all stocks for the selected time period, and writes a .avro file containing the data.

    Args:
        start_date (string): Start date in YYY-MMM-DD format
        end_date (string): End date in YYY-MM-DD format
    """
    year_data = download_year_data(start_date, end_date)

    # Para comprobar el formato del DataFrame resultante
    # year_data.to_csv("prueba.csv")

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

    with open(f"data_{start_date}_{end_date}", "wb") as out:
        writer(out, parsed_schema, records)

    return

def main():

    write_avro_file("2023-01-01", "2024-01-01")

    return


if __name__ == "__main__":
    main()