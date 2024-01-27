from get_dataframe import get_final_dataframe
import pandas as pd


if __name__ == "__main__":
    year = 0
    start_date = f"{year}-01-01"
    end_date = f"{year+1}-01-01"

    # # 1. 2018 - Save Data to .avro
    # year = 2018
    # df = get_final_dataframe(start_date, end_date)

    # # 2. 2019 - Save Data to .parquet
    # year = 2019
    # df = get_final_dataframe(start_date, end_date)

    # # 3. 2020 - Save Data to .csv
    # year = 2020
    # start_date = f"{year}-01-01"
    # end_date = f"{year+1}-01-01"
    # df = get_final_dataframe(start_date, end_date)
    # df.to_csv(f"data/stock_data_{year}.csv", index=False)

    # # 4. 2021 - Save Data to .json
    # year = 2021
    # start_date = f"{year}-01-01"
    # end_date = f"{year+1}-01-01"
    # df = get_final_dataframe(start_date, end_date)
    # df.to_json(f"data/stock_data_{year}.json", orient="records")
    
    # # 5. 2022 - Save Data to .orc
    # year = 2022
    # start_date = f"{year}-01-01"
    # end_date = f"{year+1}-01-01"
    # df = get_final_dataframe(start_date, end_date)
    # df.to_orc(f"data/stock_data_{year}.orc", index=False)

    # # 6. 2023 - Save Data to .xlsx
    # year = 2023
    # start_date = f"{year}-01-01"
    # end_date = f"{year+1}-01-01"
    # df = get_final_dataframe(start_date, end_date)
    # df.to_excel(f"data/stock_data_{year}.xlsx", index=False)
