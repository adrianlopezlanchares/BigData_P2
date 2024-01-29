from fastavro import parse_schema, writer
import csv
import json

# own imports
from data_download import *



def write_avro_file(start_date: str, end_date: str):
    """Downloads data from all stocks for the selected time period, and writes a .avro file containing the data.

    Args:
        start_date (string): Start date in YYY-MMM-DD format
        end_date (string): End date in YYY-MM-DD format
    """
    year = start_date[:4]

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


    with open(f"data/stock_data_{year}.avro", "wb") as out:
        for df in get_companies_dataframe(start_date, end_date):
            records = df.to_dict("records")
            writer(out, parsed_schema, records)
            
            break

    with open(f"data/stock_data_{year}.avro", "a+b") as out:
        i = 0
        for df in get_companies_dataframe(start_date, end_date):
            if i != 0:
                records = df.to_dict("records")
                writer(out, parsed_schema, records)
            i += 1
            


    return

def write_csv_file(start_date: str, end_date: str):
    """Downloads data from the selected year and saves it in a .csv file

    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
    """

    year = start_date[:4]
    
    with open(f"data/stock_data_{year}.csv", "w") as file:
        writer = csv.writer(file)

        i = 0

        for df in get_companies_dataframe(start_date=start_date, end_date=end_date):
            if i == 0:
                writer.writerow(df.columns)

            for _, row in df.iterrows():
                writer.writerow(row)
            i += 1


    return

def write_json_file(start_date: str, end_date: str):
    """Downloads data from the selected year and saves it in a .JSON file

    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
    """

    year = start_date[:4]
    with open(f"data/stock_data_{year}.json", 'w') as file:
        # Start writing the JSON array
        file.write('[')
        
        # Iterate through the generator function and write each DataFrame to the file
        for i, df in enumerate(get_companies_dataframe(start_date, end_date)):
            # Convert the DataFrame to a dictionary and write it to the file
            df_dict = df.to_dict(orient='records')
            json.dump(df_dict, file, indent=4)
            
            # Only write ',' if it's not the last item (there are 52 stocks)
            if i < 52:
                file.write(',')

        # End the JSON array
        file.write(']')

    return

def write_xlsx_file(start_date: str, end_date: str):
    """Downloads data from the selected year and saves it in a .xlsx file

    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
    """

    year = start_date[:4]
    current_row = 0
    
    with pd.ExcelWriter(f"data/stock_data_{year}.xlsx", engine='xlsxwriter') as writer:
        for i, df in enumerate(get_companies_dataframe(start_date, end_date)):
            
            # Write the DataFrame to the Excel file
            df.to_excel(writer, sheet_name='Sheet', index=False, startrow=current_row, header=(current_row==0))

            current_row += len(df)

    return

def main():

    write_avro_file("2018-01-01", "2019-01-01")
    write_csv_file("2019-01-01", "2020-01-01")
    write_json_file("2020-01-01", "2021-01-01")
    write_xlsx_file("2021-01-01", "2022-01-01")

    return


if __name__ == "__main__":
    main()