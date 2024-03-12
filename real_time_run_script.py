import time
from kafka import KafkaProducer
from utils_real_time import extract_daily_data, send_to_kafka


def run_script():
    today = time.strftime("%Y-%m-%d")

    producer = KafkaProducer(bootstrap_servers='192.168.80.34')

    companies_day_data = extract_daily_data(today)

    if companies_day_data is not None:
        send_to_kafka(companies_day_data, "ConsumerDiscretionary", producer)
        print("Data sent to kafka")
        print(companies_day_data)


if __name__ == "__main__":
    run_script()