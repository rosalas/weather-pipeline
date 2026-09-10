import asyncio
import logging
import os
import re
import time
from collections.abc import AsyncGenerator, Generator

import httpx
import pandas
from dotenv import load_dotenv

# Load configuration from .env file
load_dotenv()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "25.0"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "100"))

# Configure logging to write logs to pipeline.log
logging.basicConfig(
    filename="pipeline.log",
    level=getattr(logging, LOG_LEVEL.upper()),
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)
logger.info("Pipeline configuration successfully loaded.")

# ----------------------------------------------------------------------------------------------------------------------------------


# Reads input CSV all at once and normalizes messy string formatting
def clean_city_data(filepath: str) -> pandas.DataFrame:
    logger.info(f"Loading raw city data from {filepath}")
    data_frame = pandas.read_csv(filepath)

    # 1. Strip special characters using regular expressions
    data_frame["CityName"] = data_frame["CityName"].apply(
        lambda x: re.sub(r"[^a-zA-Z\s]", "", str(x))
    )

    # 2. Strip leading/trailing whitespace and convert to Title Case
    data_frame["CityName"] = data_frame["CityName"].str.strip().str.title()

    logger.info(f"Cleaned {len(data_frame)} city records.")
    return data_frame


# Reads input CSV chunk by chunk and normalizes messy string formatting
def clean_city_data_stream(
    filepath: str, chunksize: int = CHUNK_SIZE
) -> Generator[pandas.DataFrame, None, None]:
    logger.info(f"Streaming raw city data from {filepath} in chunks of {chunksize}...")

    for chunk in pandas.read_csv(filepath, chunksize=chunksize):
        # 1. Strip special characters using regular expressions
        chunk["CityName"] = chunk["CityName"].apply(
            lambda x: re.sub(r"[^a-zA-Z\s]", "", str(x))
        )

        # 2. Strip leading/trailing whitespace and convert to Title Case
        chunk["CityName"] = chunk["CityName"].str.strip().str.title()

        logger.info(f"Cleaned chunk of {len(chunk)} city records.")
        yield chunk


# ----------------------------------------------------------------------------------------------------------------------------------


# Fetches hourly weather for a single city with automated retries
# This is the sync version of this function
def fetch_weather(client: httpx.Client, city: str, lat: float, lon: float) -> dict:
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m,precipitation&timezone=auto"

    for attempt in range(1, (MAX_RETRIES + 1)):
        try:
            response = client.get(url, timeout=10.0)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully fetched weather data for {city}")
            return {"city": city, "data": data.get("hourly")}
        except httpx.HTTPError as e:
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES} failed for {city}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(1)

        logger.error(f"All retries failed for {city}.")
        return {"city": city, "data": None}


# Fetches hourly weather for a single city with automated retries
# This is the async version of this function
async def fetch_weather_async(
    client: httpx.AsyncClient, city: str, lat: float, lon: float
) -> list:
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m,precipitation&timezone=auto"

    for attempt in range(1, (MAX_RETRIES + 1)):
        try:
            response = await client.get(url, timeout=10.0)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully fetched weather data for {city}")
            return {"city": city, "data": data.get("hourly")}
        except httpx.HTTPError as e:
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES} failed for {city}: {e}")
            await asyncio.sleep(3)


# ----------------------------------------------------------------------------------------------------------------------------------


# Executes sequential API calls across all cities in the DataFrame parameter one by one
# This is the sync version of this function
def fetch_weather_all_cities(cities_data_frame: pandas.DataFrame) -> list:
    results = []

    with httpx.Client() as client:
        for _, row in cities_data_frame.iterrows():
            result = fetch_weather(client, row["CityName"], row["Lat"], row["Lon"])
            results.append(result)

    return results


# Executes concurrent API calls across all cities in the DataFrame parameter one by one
# This is the async version of this function
async def fetch_weather_all_cities_async(cities_data_frame: pandas.DataFrame) -> list:
    async with httpx.AsyncClient() as client:
        results = [
            fetch_weather_async(client, row["CityName"], row["Lat"], row["Lon"])
            for _, row in cities_data_frame.iterrows()
        ]
        return await asyncio.gather(*results)


# ----------------------------------------------------------------------------------------------------------------------------------


# Executes sequential API calls across all cities in the DataFrame parameter one by one, chunk by chunk
# This is the sync version of this function
def fetch_weather_all_cities_stream(
    city_chunks: Generator[pandas.DataFrame, None, None],
) -> Generator[dict, None, None]:
    with httpx.Client() as client:
        for chunk in city_chunks:
            for _, row in chunk.iterrows():
                result = fetch_weather(client, row["CityName"], row["Lat"], row["Lon"])
                city = result["city"]
                data = result["data"]

                if not data:
                    continue

                times = data.get("time", [])
                temps = data.get("temperature_2m", [])
                precips = data.get("precipitation", [])

                for t, temp, precip in zip(times, temps, precips):
                    yield {"City": city, "Time": t, "Temp_C": temp, "Precip_mm": precip}


# Executes concurrent API calls across all cities in the DataFrame parameter, chunk by chunk
# This is the async version of this function
async def fetch_weather_all_cities_stream_async(
    city_chunks: Generator[pandas.DataFrame, None, None],
) -> AsyncGenerator[dict, None]:
    """Asynchronously fetches weather data concurrently for each chunk and streams records."""
    async with httpx.AsyncClient() as client:
        for chunk in city_chunks:
            # 1. Fire concurrent API requests for all cities in the current chunk
            tasks = [
                fetch_weather(client, row["CityName"], row["Lat"], row["Lon"])
                for _, row in chunk.iterrows()
            ]
            results = await asyncio.gather(*tasks)

            # 2. Process results and stream individual records
            for result in results:
                city = result["city"]
                data = result["data"]

                if not data:
                    continue

                times = data.get("time", [])
                temps = data.get("temperature_2m", [])
                precips = data.get("precipitation", [])

                for t, temp, precip in zip(times, temps, precips):
                    yield {"City": city, "Time": t, "Temp_C": temp, "Precip_mm": precip}


# ----------------------------------------------------------------------------------------------------------------------------------


# Transforms raw API responses into aggregated data structures
# Exports reports
def transform_and_export(raw_results: list):
    all_records = []

    for result in raw_results:
        city = result["city"]
        data = result["data"]

        if not data:
            continue

        times = data["time"]
        temps = data["temperature_2m"]
        precips = data["precipitation"]

        for t, temp, precip in zip(times, temps, precips):
            all_records.append(
                {"City": city, "Time": t, "Temp_C": temp, "Precip_mm": precip}
            )

    if not all_records:
        logger.error("No valid records found to transform.")

    data_frame = pandas.DataFrame(all_records)

    # Date/time handling
    data_frame["Time"] = pandas.to_datetime(data_frame["Time"])
    data_frame["Date"] = data_frame["Time"].dt.date

    # Grouping and aggregation
    daily_summary = (
        data_frame.groupby(["City", "Date"])
        .agg(Max_Temp_C=("Temp_C", "max"), Total_Precip_mm=("Precip_mm", "sum"))
        .reset_index()
    )

    # 1. Excel report
    os.makedirs("reports", exist_ok=True)
    excel_path = "reports/weather_summary.xlsx"
    daily_summary.to_excel(excel_path, index=False)
    logger.info(f"Excel report saved to {excel_path}")

    # 2. JSON report
    alerts = daily_summary[daily_summary["Max_Temp_C"] > ALERT_THRESHOLD]
    json_path = "reports/alerts.json"
    alerts.to_json(json_path, orient="records", indent=2)
    logger.info(f"JSON report saved to {json_path}")


# Internal function that summarizes and deduplicates a batch of data
def _summarize_batch(batch: list) -> pandas.DataFrame:
    data_frame = pandas.DataFrame(batch)
    data_frame["Time"] = pandas.to_datetime(data_frame["Time"])
    data_frame["Date"] = data_frame["Time"].dt.date
    return (
        data_frame.groupby(["City"], ["Date"])
        .agg(Max_Temp_C=("Temp_C", "max"), Total_Precip_mm=("Precip_mm", "sum"))
        .reset_index()
    )


def transform_and_export_stream(
    record_stream: Generator[dict, None, None], batch_size: int = 100
):
    daily_summaries = []
    current_batch = []

    for record in record_stream:
        current_batch.append(record)

        if len(current_batch) >= batch_size:
            daily_summaries.append(_summarize_batch(current_batch))

        if not daily_summaries:
            logger.error("No valid records found to transform.")
            return

        # Combine partial summaries into final DataFrame
        combined_summary = (
            pandas.concat(daily_summaries, ignore_index=True)
            .groupby(["City"], ["Date"])
            .agg(Max_Temp_C=("Temp_C", "max"), Total_Precip_mm=("Precip_mm", "sum"))
            .reset_index()
        )

        # 1. Excel report
        os.makedirs("reports", exist_ok=True)
        excel_path = "reports/weather_summary.xlsx"
        combined_summary.to_excel(excel_path, index=False)
        logger.info(f"Excel report saved to {excel_path}")

        # 2. JSON report
        alerts = combined_summary[combined_summary["Max_Temp_C"] > ALERT_THRESHOLD]
        json_path = "reports/alerts.json"
        alerts.to_json(json_path, orient="records", indent=2)
        logger.info(f"JSON report saved to {json_path}")


# ----------------------------------------------------------------------------------------------------------------------------------


def main():
    # Main pipeline execution flow
    logger.info("Starting weather pipeline benchmark...")

    # 1. Load and clean input city data
    cities_data_frame = clean_city_data("data/cities.csv")

    # 2.1. Synchronous execution & timing
    logger.info("Beginning sync weather data extraction...")
    start_sync = time.perf_counter()
    fetch_weather_all_cities(cities_data_frame)
    sync_duration = time.perf_counter() - start_sync
    logger.info(f"Synchronous fetching completed in {sync_duration:.2f} seconds.")

    # 2.2. Asynchronous execution & timing
    logger.info("Beginning async weather data extraction...")
    start_async = time.perf_counter()
    async_results = asyncio.run(fetch_weather_all_cities_async(cities_data_frame))
    async_duration = time.perf_counter() - start_async
    logger.info(f"Asynchronous fetching completed in {async_duration:.2f} seconds.")

    # 3. Performance comparison & logging
    time_saved = sync_duration - async_duration
    speedup = sync_duration / async_duration if async_duration > 0 else 1.0

    logger.info("================ Performance Benchmark Summary ================")
    logger.info(f"Sync execution time : {sync_duration:.2f} seconds")
    logger.info(f"Async execution time: {async_duration:.2f} seconds")
    logger.info(f"Time saved          : {time_saved:.2f} seconds")
    logger.info(f"Async performance   : {speedup:.2f}x faster than synchronous")
    logger.info("==============================================================")

    # 4. Transform data and generate reports
    transform_and_export(async_results)

    logger.info("Pipeline execution completed successfully.")


# Python idiom to ensure the main() function is not executed
# if the script is imported as a module elsewhere
if __name__ == "__main__":
    main()
