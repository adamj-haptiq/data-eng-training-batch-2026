import requests
import time

def fetch_weather_data(city: str):
    """Simulate expensive weather API call"""
    print(f"Fetching weather for {city}...")
    time.sleep(2)  # Simulate API delay

    # Mock weather data
    return {
        "city": city,
        "temperature": 72,
        "humidity": 65,
        "description": "Partly cloudy"
    }

def weather_report(cities: list):
    """Generate weather report for multiple cities"""
    results = []
    for city in cities:
        # This will make redundant calls for repeated cities
        weather = fetch_weather_data(city)
        results.append(weather)
    return results

if __name__ == "__main__":
    cities = ["New York", "London", "Tokyo", "New York", "London"]  # Notice duplicates
    start_time = time.time()
    report = weather_report(cities)
    end_time = time.time()

    print(f"Report generated in {end_time - start_time:.2f} seconds")
    for weather in report:
        print(f"{weather['city']}: {weather['temperature']}°F")