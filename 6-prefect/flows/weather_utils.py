import json
import os
from typing import Any

import psycopg2
import requests
from prefect import get_run_logger

from transformer import transform_weatherAPI


def extract_weather_data(location: str = "Berlin") -> dict[str, Any]:
    """Pull current weather observation from WeatherAPI."""
    api_key = os.getenv("WEATHER_API_KEY")
    if not api_key:
        raise ValueError(
            "WEATHER_API_KEY is not set. Add it to your .env before running weather flows."
        )
    payload = {"key": api_key, "q": location, "aqi": "no"}
    response = requests.get(
        "http://api.weatherapi.com/v1/current.json", params=payload, timeout=30
    )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        if response.status_code in (401, 403):
            raise RuntimeError(
                "WeatherAPI rejected WEATHER_API_KEY. Update WEATHER_API_KEY in .env."
            ) from exc
        raise
    return response.json()


def transform_weather_data(weather_json: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalize weather API response into a compact record list."""
    weather_str = json.dumps(weather_json)
    transformed_str = transform_weatherAPI(weather_str)
    return json.loads(transformed_str)


def load_weather_data(weather_data: list[dict[str, Any]], host: str = "postgres") -> int:
    """Insert transformed weather records into Postgres and return inserted row count."""
    if not weather_data:
        return 0

    db_host = os.getenv("DB_HOST", host)
    db_port = int(os.getenv("DB_PORT", "5432"))
    db_user = os.getenv("DB_USER", "prefect")
    db_password = os.getenv("DB_PASSWORD", "prefect")
    db_name = os.getenv("DB_NAME", "prefect")

    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database=db_name,
        )
        cursor = connection.cursor()
        cursor.execute("CREATE SCHEMA IF NOT EXISTS weatherdata;")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS weatherdata.temperature (
                id BIGSERIAL PRIMARY KEY,
                location TEXT NOT NULL,
                temp_c NUMERIC(10, 4) NOT NULL,
                wind_kph NUMERIC(10, 4) NOT NULL,
                time TIMESTAMP NOT NULL
            );
            """
        )

        insert_query = """
            INSERT INTO weatherdata.temperature (location, temp_c, wind_kph, time)
            VALUES (%s, %s, %s, %s);
        """
        for record in weather_data:
            cursor.execute(
                insert_query,
                (
                    record["location"],
                    record["temp_c"],
                    record["wind_kph"],
                    record["timestamp"],
                ),
            )

        connection.commit()
        return len(weather_data)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def log_weather_data(weather_data: list[dict[str, Any]]) -> None:
    """Log transformed weather records to Prefect task logs."""
    logger = get_run_logger()
    logger.info("Weather records: %s", weather_data)
