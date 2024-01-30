from fastavro import parse_schema, writer
import pyarrow as pa
import pyarrow.parquet as pq
import csv
import json
import pyorc
import pandas as pd

# own imports
from data_download import *


class DataWriter(object):
    """Class that handles writing data to files."""

    def __init__(self, path):
        """Constructor for DataWriter class."""
        self.path = path
        return
    


    def write_avro_file(self, start_date: str, end_date: str):
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
                {"name": "CIK", "type": "string"},
                {"name": "Open", "type": "float"},
                {"name": "High", "type": "float"},
                {"name": "Low", "type": "float"},
                {"name": "Close", "type": "float"},
                {"name": "Adj_Close", "type": "float"},
                {"name": "Volume", "type": "float"},
            ]
        }
        parsed_schema = parse_schema(schema)


        with open(f"{self.path}/stock_data_{year}.avro", "wb") as out:
            for df in get_companies_dataframe(start_date, end_date):
                records = df.to_dict("records")
                writer(out, parsed_schema, records)
                
                break

        with open(f"{self.path}stock_data_{year}.avro", "a+b") as out:
            i = 0
            for df in get_companies_dataframe(start_date, end_date):
                if i != 0:
                    records = df.to_dict("records")
                    writer(out, parsed_schema, records)
                i += 1

        return


    def write_parquet_file(self, start_date: str, end_date: str):
        """Downloads data from the selected year and saves it in a .parquet dataset.

        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format    
        """
        
        year = start_date[:4]

        for df in get_companies_dataframe(start_date, end_date):
            table = pa.Table.from_pandas(df)
            pq.write_to_dataset(table, f'{self.path}/stock_data_{year}.parquet')

        return


    def write_csv_file(self, start_date: str, end_date: str):
        """Downloads data from the selected year and saves it in a .csv file

        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
        """

        year = start_date[:4]
        
        with open(f"{self.path}/stock_data_{year}.csv", "w", newline='') as file:
            writer = csv.writer(file)

            first_line = True

            for df in get_companies_dataframe(start_date=start_date, end_date=end_date):
                if first_line:
                    writer.writerow(df.columns)
                    first_line = False

                for _, row in df.iterrows():
                    writer.writerow(row)

        return


    def write_json_file(self, start_date: str, end_date: str):
        """Downloads data from the selected year and saves it in a .JSON file

        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
        """

        year = start_date[:4]
        with open(f"{self.path}/stock_data_{year}.json", 'w') as file:
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


    def write_orc_file(self, start_date: str, end_date: str):
        """Downloads data from the selected year and saves it in a .orc file

        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
        """
        
        schema = "struct<Date:string,Ticker:string,CIK:string,Open:float,High:float,Low:float,Close:float,Adj_Close:float,Volume:int>"

        year = start_date[:4]
        with open(f"{self.path}/stock_data_{year}.orc", "wb") as file:
            with pyorc.Writer(file, schema) as writer:
                for df in get_companies_dataframe(start_date, end_date):
                    records = df.to_dict("records")
                    for record in records:
                        # We get the values because the pyorc writer expects a tuple
                        # (the schema is already defined, so we don't need the keys)
                        record = tuple(record.values())
                        writer.write(record)
        
        return


    def write_xlsx_file(self, start_date: str, end_date: str):
        """Downloads data from the selected year and saves it in a .xlsx file

        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
        """

        year = start_date[:4]
        current_row = 0
        
        with pd.ExcelWriter(f"{self.path}/stock_data_{year}.xlsx") as writer:
            for df in get_companies_dataframe(start_date, end_date):
                
                # Write the DataFrame to the Excel file
                df.to_excel(writer, sheet_name='Sheet', index=False, startrow=current_row, header=(current_row==0))

                current_row += len(df)

        return
