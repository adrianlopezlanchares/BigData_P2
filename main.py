import sys 
import json

# own imports
from datawriter import DataWriter


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

    if file_format == 'csv':
        dataWriter.write_csv_file(start_date, end_date)
    elif file_format == 'json':
        dataWriter.write_json_file(start_date, end_date)
    elif file_format == 'orc':
        dataWriter.write_orc_file(start_date, end_date)
    elif file_format == 'xlsx' or file_format == 'excel':
        dataWriter.write_xlsx_file(start_date, end_date)
    elif file_format == 'parquet':
        dataWriter.write_parquet_file(start_date, end_date)
    else:
        success = False

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

    dataWriter = DataWriter(path)


    file_format = sys.argv[1]
    year = sys.argv[2]

    if write_selected_data(dataWriter, file_format, year):
        print("Data downloaded and written successfuly")
    else:
        print("There was an error. The format for the command is: python3 main.py {file_format} {year}")

    return


if __name__ == "__main__":
    main()
