from write_data_files import DataWriter


def main():

    dw = DataWriter()

    dw.write_avro_file("2018-01-01", "2019-01-01")
    dw.write_parquet_file("2019-01-01", "2020-01-01")
    dw.write_csv_file("2020-01-01", "2021-01-01")
    dw.write_json_file("2021-01-01", "2022-01-01")
    dw.write_orc_file("2022-01-01", "2023-01-01")
    dw.write_xlsx_file("2023-01-01", "2024-01-01")

    return


if __name__ == "__main__":
    main()
