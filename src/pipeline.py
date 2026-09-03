import time
import logging
import os
import re
import httpx
import pandas
from dotenv import load_dotenv

# Load configuration from .env file
load_dotenv()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", 25.0))

# Configure logging to write logs to pipeline.log
logging.basicConfig(
    filename = "pipeline.log",
    level = getattr(logging, LOG_LEVEL.upper()),
    format = "%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Pipeline configuration successfully loaded.")

# Reads input CSV and normalizes messy string formatting
def clean_city_data(filepath: str) -> pandas.DataFrame:
    logging.info(f"Loading raw city data from {filepath}")
    data_frame = pandas.read_csv(filepath)

    # 1. Strip special characters using regular expressions
    data_frame['CityName'] = data_frame['CityName'].apply(
        lambda x: re.sub(r'[^a-zA-Z\s]', '', str(x))
    )

    # 2. Strip leading/trailing whitespace and convert to Title Case
    data_frame['CityName'] = data_frame['CityName'].str.strip().str.title()

    logging.info(f"Cleaned {len(data_frame)} city records.")
    return data_frame

# Fetches hourly weather for a single city with automated retries
# This is the sync version of this function 
def fetch_weather(client: httpx.Client, city: str, lat: float, lon: float) -> dict:
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m,precipitation&timezone=auto"

    for attempt in range(1, (MAX_RETRIES + 1)):
        try:
            response = client.get(url, timeout = 10.0)
            response.raise_for_status()
            data = response.json()
            logging.info(f"Successfully fetched weather data for {city}")
            return {"city": city, "data": data.get("hourly")}
        except httpx.HTTPError as e:
            logging.warning(f"Attempt {attempt}/{MAX_RETRIES} failed for {city}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(1)

        logging.error(f"All retries failed for {city}.")
        return {"city": city, "data": None}

# Executes sequential API calls across all cities in the DataFrame parameter one by one
# THis is the sync version of this function
def fetch_weather_all_cities(cities_data_frame: pandas.DataFrame) -> list:
    results = []

    with httpx.Client() as client:
        for _, row in cities_data_frame.iterrows():
            result = fetch_weather(client, row['CityName'], row['Lat'], row['Lon'])
            results.append(result)

    return results

def transform_and_export(raw_results: list):
    # Transforms raw API responses into aggregated data structures
    # Exports reports
    all_records = []

    for result in raw_results:
        city = result['city']
        data = result['data']

        if not data:
            continue

        times = data['time']
        temps = data['temperature_2m']
        precips = data['precipitation']

        for time, temp, precip in zip(times, temps, precips):
            all_records.append({
                "City": city,
                "Time": time,
                "Temp_C": temp,
                "Precip_mm": precip
            })

    if not all_records:
        logging.error("No valid records found to transform.")

    data_frame = pandas.DataFrame(all_records)

    # Date/time handling
    data_frame['Time'] = pandas.to_datetime(data_frame['Time'])
    data_frame['Date'] = data_frame['Time'].dt.date

    # Grouping and aggregation
    daily_summary = data_frame.groupby(['City', 'Date']).agg(
        Max_Temp_C = ('Temp_C', 'max'),
        Total_Precip_mm = ('Precip_mm', 'sum')
    ).reset_index()

    # 1. Excel report
    excel_path = "reports/weather_summary.xlsx"
    daily_summary.to_excel(excel_path, index = False)
    logging.info(f"Excel report saved to {excel_path}")

    # 2. JSON report
    alerts = daily_summary[daily_summary['Max_Temp_C'] > ALERT_THRESHOLD]
    json_path = "reports/alerts.json"
    alerts.to_json(json_path, orient = "records", indent = 2)
    logging.info(f"JSON report saved to {json_path}")

def main():
    # Main pipeline execution flow
    logging.info("Starting weather pipeline...")

    # Load and clean input data
    cities_data_frame = clean_city_data("data/cities.csv")

    # Fetch weather data sequentially
    # This is the sync version of this step
    results = fetch_weather_all_cities(cities_data_frame)

    # Transform data and generate reports
    transform_and_export(results)

    logging.info("Pipeline execution completed successfully.")

# Python idiom to ensure the main() function is not executed
# if the script is imported as a module elsewhere
if __name__ == "__main__":
    main()