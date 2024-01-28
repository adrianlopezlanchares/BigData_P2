from data_download_03_04_06 import get_companies_dataframe
import pandas as pd


def main():
    
    # 3. 2020 - Save Data to .csv
    year = 2020
    start_date = f"{year}-01-01"
    end_date = f"{year+1}-01-01"
    df = get_companies_dataframe(start_date, end_date)
    df.to_csv(f"data/stock_data_{year}.csv", index=False)

    # 4. 2021 - Save Data to .json
    year = 2021
    start_date = f"{year}-01-01"
    end_date = f"{year+1}-01-01"
    df = get_companies_dataframe(start_date, end_date)
    df.to_json(f"data/stock_data_{year}.json", orient="records")
    
    # 6. 2023 - Save Data to .xlsx
    year = 2023
    start_date = f"{year}-01-01"
    end_date = f"{year+1}-01-01"
    df = get_companies_dataframe(start_date, end_date)
    df.to_excel(f"data/stock_data_{year}.xlsx", index=False)


if __name__ == "__main__":
    main()
