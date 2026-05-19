import requests
import time
from prefect import flow, task
from datetime import timedelta

def name_weather_flow():
    """Generate dynamic flow name"""
    from datetime import datetime
    return f"weather-report-{datetime.now().strftime('%Y-%m-%d')}"

@task(cache_key_fn=lambda city: f"weather_{city}", cache_expiration=timedelta(hours=1))
def fetch_weather_data(city: str):
    """Fetch weather data with caching"""
    print(f"Fetching weather for {city}...")
    time.sleep(2)  # Simulate API delay

    # Mock weather data
    return {
        "city": city,
        "temperature": 72,
        "humidity": 65,
        "description": "Partly cloudy"
    }

@flow(flow_run_name=name_weather_flow, log_prints=True)
def weather_report_flow(cities: list):
    """Generate weather report with intelligent caching"""
    results = []
    for city in cities:
        # Cached calls will be instant for repeated cities
        weather = fetch_weather_data(city)
        results.append(weather)
    return results

if __name__ == "__main__":
    cities = ["New York", "London", "Tokyo", "New York", "London"]  # Duplicates will use cache
    start_time = time.time()
    report = weather_report_flow(cities)
    end_time = time.time()

    print(f"Report generated in {end_time - start_time:.2f} seconds")
    for weather in report:
        print(f"{weather['city']}: {weather['temperature']}°F")