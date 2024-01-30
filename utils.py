from data_writer import DataWriter
import os


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


def write_selected_data(dataWriter: DataWriter, file_format: str, year: int):
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

    return


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
