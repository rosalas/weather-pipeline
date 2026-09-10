import json
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.pipeline import (
    clean_city_data,
    fetch_weather,
    fetch_weather_async,
    transform_and_export,
)


# 1. Unit test for data normalization
# Verifies that the city name cleaning logic handles all relevant cases
def test_clean_city_data(tmp_path):
    # Create a test CSV file
    test_csv = tmp_path / "test_cities.csv"
    test_csv.write_text("CityName,Lat,Lon\n  pArIS!!  ,48.85,2.35\nTOkyO,35.68,139.69")

    # Execute the function on the test CSV
    data_frame = clean_city_data(str(test_csv))

    assert list(data_frame["CityName"]) == ["Paris", "Tokyo"]
    assert len(data_frame) == 2


# 2.1. Unit test for API mocking
# Mocks an HTTP GET response to verify API handling before reaching Open-Meteo
# This unit test works for the sync version of the API integration
@patch("src.pipeline.httpx.Client.get")
def test_fetch_weather(mock_get):
    # Create a mock HTTP GET response
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "hourly": {
            "time": ["2026-01-01T00:00"],
            "temperature_2m": [22.5],
            "precipitation": [0.0],
        }
    }

    # Create a mock HTTP GET client
    mock_client = MagicMock()
    mock_client.get.return_value = mock_response

    # Execute the function on the mock client and mock response data
    result = fetch_weather(mock_client, "Paris", 48.85, 2.35)

    assert result["city"] == "Paris"
    assert result["data"]["temperature_2m"][0] == 22.5


# 2.2. Unit test for API mocking
# Mocks an HTTP GET response to verify API handling before reaching Open-Meteo
# This unit test works for the async version of the API integration
@pytest.mark.asyncio
@patch("src.pipeline.httpx.Client.get")
async def test_fetch_weather_async(mock_get):
    # Create a mock HTTP GET response
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "hourly": {
            "time": ["2026-01-01T00:00"],
            "temperature_2m": [22.5],
            "precipitation": [0.0],
        }
    }

    # Create a mock HTTP GET client
    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response

    # Execute the function on the mock client and mock response data
    result = await fetch_weather_async(mock_client, "Paris", 48.85, 2.35)

    assert result["city"] == "Paris"
    assert result["data"]["temperature_2m"][0] == 22.5


# 3. Unit test for data transformation and exports
# Verifies pandas aggregations and checks that both the Excel and JSON reports are generated
def test_transform_and_export(tmp_path, monkeypatch):
    # Redirect file outputs to pytest's temporary directory
    monkeypatch.chdir(tmp_path)

    # Create sample raw results
    sample_raw_results = [
        {
            "city": "Paris",
            "data": {
                "time": ["2026-01-01T00:00", "2026-01-01T01:00"],
                "temperature_2m": [
                    15.0,
                    30.0,
                ],  # 30.0 exceeds the established threshold
                "precipitation": [1.0, 2.0],
            },
        }
    ]

    # Execute the function on the sample raw results
    transform_and_export(sample_raw_results)

    # Verify report files were created
    assert os.path.exists("reports/weather_summary.xlsx")
    assert os.path.exists("reports/alerts.json")

    # Read generated JSON alert file to verify contents
    with open("reports/alerts.json", "r") as f:
        alert_data = json.load(f)

    assert len(alert_data) == 1
    assert alert_data[0]["City"] == "Paris"
    assert alert_data[0]["Max_Temp_C"] == 30.0
