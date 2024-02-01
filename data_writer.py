from fastavro import parse_schema, writer
import pyarrow as pa
import pyarrow.parquet as pq
import csv
import json
import pyorc
import pandas as pd

# own imports
from utils import get_companies_dataframe


class DataWriter(object):
    """Class that handles writing data to files."""

    def __init__(self, path):
        """Constructor for DataWriter class."""
        self.path = path
        return
    
    def write_all_data(self) -> bool:
        """This function writes all the data in all the formats

        """
        self.write_avro_file("2018-01-01", "2019-01-01")
        self.write_parquet_file("2019-01-01", "2020-01-01")
        self.write_csv_file("2020-01-01", "2021-01-01")
        self.write_json_file("2021-01-01", "2022-01-01")
        self.write_orc_file("2022-01-01", "2023-01-01")
        self.write_xlsx_file("2023-01-01", "2024-01-01")

        return
    
    def write_selected_data(self, file_format: str, year: int):
        """Given the file format and the year, the function writes the data from said year into
        the correct file format

        Args:
            dataWriter (DataWriter): DataWriter class that will write the data into the file
            file_format (str): Selected file format (CSV, JSON, ORC, XLSX, AVRO, Parquet)
            year (int): Year to download the data from
        """

        start_date = f"{year}-01-01"
        end_date = f"{int(year) + 1}-01-01"

        file_format = file_format.lower()

        if file_format == 'avro':
            self.write_avro_file(start_date, end_date)
        elif file_format == 'parquet':
            self.write_parquet_file(start_date, end_date)
        if file_format == 'csv':
            self.write_csv_file(start_date, end_date)
        elif file_format == 'json':
            self.write_json_file(start_date, end_date)
        elif file_format == 'orc':
            self.write_orc_file(start_date, end_date)
        elif file_format == 'xlsx' or file_format == 'excel':
            self.write_xlsx_file(start_date, end_date)

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


        with open(f"{self.path}stock_data_{year}.avro", "wb") as out:
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

        first_iter = True


        for df in get_companies_dataframe(start_date, end_date):
            # table = pa.Table.from_pandas(df)
            # pq.write_to_dataset(table, f'{self.path}stock_data_{year}.parquet')
            if first_iter:
                df_total = df
                first_iter = False
            
            else:
                df_total = pd.concat([df_total, df], ignore_index=True)

        df_total.to_parquet(f"{self.path}/stock_data_{year}.parquet")

        return

    def write_csv_file(self, start_date: str, end_date: str):
        """Downloads data from the selected year and saves it in a .csv file

        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
        """

        year = start_date[:4]
        
        with open(f"{self.path}stock_data_{year}.csv", "w", newline='') as file:
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
        with open(f"{self.path}stock_data_{year}.json", 'w') as file:
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
        with open(f"{self.path}stock_data_{year}.orc", "wb") as file:
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
        
        with pd.ExcelWriter(f"{self.path}stock_data_{year}.xlsx") as writer:
            for df in get_companies_dataframe(start_date, end_date):
                
                # Write the DataFrame to the Excel file
                df.to_excel(writer, sheet_name='Sheet', index=False, startrow=current_row, header=(current_row==0))

                current_row += len(df)

        return