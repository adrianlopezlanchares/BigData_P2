import sys 
import json

# own imports
from data_writer import DataWriter
from utils import control_data_directory


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
        dataWriter.write_all_data(dataWriter)

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
                dataWriter.write_selected_data(dataWriter, file_format, year)
            else:
                raise ValueError("The year is not correct. It must be between 2018 and 2023")
        except:
            raise ValueError("The year is not correct. It must be an integer")

    return


if __name__ == "__main__":
    main()
