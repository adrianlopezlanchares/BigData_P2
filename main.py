import sys 
import json
import os

# own imports
from data_writer import DataWriter


def write_all_data(dataWriter: DataWriter) -> bool:
    """Given the DataWriter class, the function writes all the data in all the formats
    
    Args:
        dataWriter (DataWriter): DataWriter class that will write the data into the file
    """
    dataWriter.write_avro_file("2018-01-01", "2019-01-01")
    dataWriter.write_parquet_file("2019-01-01", "2020-01-01")
    dataWriter.write_csv_file("2020-01-01", "2021-01-01")
    dataWriter.write_json_file("2021-01-01", "2022-01-01")
    dataWriter.write_orc_file("2022-01-01", "2023-01-01")
    dataWriter.write_xlsx_file("2023-01-01", "2024-01-01")

    return


def write_selected_data(dataWriter: DataWriter, file_format: str, year: int) -> bool:
    """Given the file format and the year, the function writes the data from said year into
       the correct file format

    Args:
        dataWriter (DataWriter): DataWriter class that will write the data into the file
        file_format (str): Selected file format (CSV, JSON, ORC, XLSX, AVRO, Parquet)
        year (int): Year to download the data from

    Returns:
        bool: True if process carried out succesfully, false if it didn't
    """
    success = True

    start_date = f"{year}-01-01"
    end_date = f"{int(year) + 1}-01-01"

    file_format = file_format.lower()

    if file_format == 'avro':
        dataWriter.write_avro_file(start_date, end_date)
    elif file_format == 'parquet':
        dataWriter.write_parquet_file(start_date, end_date)
    if file_format == 'csv':
        dataWriter.write_csv_file(start_date, end_date)
    elif file_format == 'json':
        dataWriter.write_json_file(start_date, end_date)
    elif file_format == 'orc':
        dataWriter.write_orc_file(start_date, end_date)
    elif file_format == 'xlsx' or file_format == 'excel':
        dataWriter.write_xlsx_file(start_date, end_date)
    else:
        success = False

    return success


def control_data_directory(path: str) -> bool:
    """Given the path, the function checks if the data directory exists. If it doesn't, it creates it

    Args:
        path (str): Path to the data directory

    Returns:
        bool: True if the directory exists or was created, false if it wasn't
    """
    success = True

    if os.path.isdir(path):
        print(f"The directory {path} already exists")
    else:
        try:
            os.mkdir(path)
        except OSError:
            print(f"Creation of the directory {path} failed")
            success = False
        else:
            print(f"Successfully created the directory {path}")

    return success


def main():
    """This function expects 2 arguments from the command line. File format and year:
       
       file_format (str): Type of file to write the data in
       year (int): Year to download the data from
    """

    with open('config.json') as config_file:
        config_file_data = config_file.read()

    data = json.loads(config_file_data)
    path = data['path']

    # Check if the data directory exists. If it doesn't, create it
    control_data_directory(path)

    dataWriter = DataWriter(path)

    command_args_received = sys.argv

    # Follow different paths depending on the number of arguments received
    if len(command_args_received) == 1:
        # If no arguments are received, write all the data in all the formats
        write_all_data(dataWriter)

    elif len(sys.argv) == 2:
        # If one argument is received, raise an error
        raise SyntaxError("Only one argument was given. The correct format is: python3 main.py <file_format> <year>")

    elif len(sys.argv) == 3:
        # If two arguments are received, write the data in the selected format
        # (if the arguments are correct)
        file_format, year = sys.argv[1:]

        # Check if the arguments are correct
        if file_format not in ["parquet", "avro", "csv", "json", "orc", "xlsx", "excel"]:
            raise ValueError("The file format is not correct. The correct formats are: parquet, avro, csv, json, orc, xlsx")
        try:
            year = int(year)
            if year in range(2018, 2024):
                write_selected_data(dataWriter, file_format, year)
            else:
                raise ValueError("The year is not correct. It must be between 2018 and 2023")
        except:
            raise ValueError("The year is not correct. It must be an integer")


    return


if __name__ == "__main__":
    main()
