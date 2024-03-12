from kafka import KafkaProducer
from utils import get_companies_dataframe
import pandas as pd


def extract_daily_data(date: str, sector: str = "Consumer Discretionary") -> dict:
    """Extracts the daily data from yfinance

    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        sector (str, optional): Sector of the companies. Defaults to "Consumer Discretionary".

    Returns:
        dict: DataFrame with the daily data
    """
    companies_day_data = {}

    yesterday = pd.to_datetime(date) - pd.Timedelta(days=1)

    yesterday = yesterday.strftime("%Y-%m-%d")

    for df in get_companies_dataframe(start_date=yesterday, end_date=date, sector=sector):
        try:
            companies_day_data[df["Ticker"].iloc[0]] = df.to_dict(orient="records")[0]
        except:
            print(f"No data from {yesterday}")

    return companies_day_data


def send_to_kafka(data: dict, topic: str, producer: KafkaProducer):
    """Sends the data to the kafka topic in json format
    
    Args:
        data (dict): Data to send
        topic (str): Topic to send the data
        producer (KafkaProducer): Kafka producer
    """
    producer.send(topic, value=pd.DataFrame.from_dict(data).to_json().encode('utf-8'))
