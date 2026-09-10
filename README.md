# Weather Analytics & Alerting Pipeline

An automated, production-grade Python pipeline designed to extract, clean, transform, and aggregate global forecast data from the Open-Meteo REST API. This project demonstrates REST API integration, regular expression text normalization, pandas data aggregation, automated multi-format reporting (Excel and JSON), unit testing with API mocking, and automated code quality checks using Ruff and GitHub Actions.

## Project Structure

```text
weather_pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml             # GitHub Actions CI workflow
├── data/
│   └── cities.csv             # Raw input dataset with dirty formatting
├── reports/
│   ├── weather_summary.xlsx   # Aggregated daily weather report
│   └── alerts.json            # High-temperature alert export
├── src/
│   ├── __init__.py            # Python package marker
│   └── pipeline.py            # Core pipeline execution script
├── tests/
│   └── test_pipeline.py       # Pytest suite with offline API mocking
├── .env                       # Environment configuration (ignored by Git)
├── .gitignore                 # Git exclusion rules
├── pyproject.toml             # Project configuration and dependencies
├── uv.lock                    # Deterministic dependency lockfile
└── README.md                  # Project documentation
```

---

## Features

* **Environment & Package Management:** Managed via `uv` for fast dependency resolution and execution using `pyproject.toml` and `uv.lock`.
* **Data Cleaning & Regex:** Uses regular expressions (`re`) and pandas string accessors (`.str`) to strip invalid characters, trim whitespace, and normalize casing.
* **API Integration & Error Handling:** Connects to Open-Meteo via `httpx` with timeout controls, HTTP status checks, and retry loops.
* **Data Transformation & Aggregation:** Parses datetime strings using pandas `.dt` accessors, calculates daily maximum temperatures and precipitation sums via `groupby.agg()`.
* **Automated Reporting:** Generates structured outputs in `.xlsx` (using `openpyxl`) and `.json`.
* **Logging & Environment Security:** Configures `logging` dynamically through `.env` environment variables using `python-dotenv`.
* **Testing & Quality Assurance:** Tested with `pytest` and `unittest.mock` to ensure offline reliability, enforced with `ruff` linting and formatting.

---

## Prerequisites & Installation

### 1. Install `uv`
Ensure the `uv` project manager is installed on your machine:

```bash
# macOS/Linux
curl -LsSf [https://astral.sh/uv/install.sh](https://astral.sh/uv/install.sh) | sh

# Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm [https://astral.sh/uv/install.ps1](https://astral.sh/uv/install.ps1) | iex"
```

### 2. Clone the Repository & Sync Environment
```bash
git clone <your-repository-url>
cd weather_pipeline
uv sync
```

### 3. Setup Configuration
Create a `.env` file in the root directory:

```env
LOG_LEVEL=INFO
MAX_RETRIES=3
ALERT_THRESHOLD=25.0
```

---

## Usage

### Run the Pipeline
To execute the complete pipeline:

```bash
uv run python src/pipeline.py
```

Upon completion:
1. Logs will be recorded in `pipeline.log`.
2. The daily weather summary will be saved to `reports/weather_summary.xlsx`.
3. High-temperature alert records will be saved to `reports/alerts.json`.

---

## Testing & Code Quality

### Run Unit Tests
Run the test suite offline using pytest:

```bash
uv run pytest
```

### Run Code Quality Checks
Verify code compliance and auto-format using Ruff:

```bash
# Check for linting errors
uv run ruff check .

# Check code formatting compliance
uv run ruff format --check .

# Automatically apply formatting fixes
uv run ruff format .
```

---

## Continuous Integration (CI)

This repository includes a GitHub Actions workflow located at `.github/workflows/ci.yml`. On every Pull Request or push to `main`, GitHub Actions automatically:
1. Provisions an Ubuntu container with Python 3.12 and `uv`.
2. Installs project dependencies via `uv sync`.
3. Executes `ruff format --check .` and `ruff check .`.
4. Runs the test suite via `uv run pytest`.