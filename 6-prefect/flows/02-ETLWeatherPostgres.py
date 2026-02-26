from typing import Any

from prefect import flow, task

from weather_utils import extract_weather_data, load_weather_data, transform_weather_data


@task
def extract_task(location: str = "Berlin") -> dict[str, Any]:
    return extract_weather_data(location)


@task
def transform_task(weather_json: dict[str, Any]) -> list[dict[str, Any]]:
    return transform_weather_data(weather_json)


@task
def load_task(weather_data: list[dict[str, Any]]) -> int:
    return load_weather_data(weather_data)


@flow(name="ETLWeatherPostgres")
def etl_weather_postgres(location: str = "Berlin") -> None:
    weather_data = extract_task(location)
    weather_summary = transform_task(weather_data)
    load_task(weather_summary)


if __name__ == "__main__":
    etl_weather_postgres()
