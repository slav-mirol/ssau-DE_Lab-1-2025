import os
import io
from datetime import datetime, timedelta
from typing import Dict, List, Any
import json
import requests

from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.blocks.notifications import NotificationBlock

from minio import Minio
from minio.error import S3Error

import clickhouse_connect

from dotenv import load_dotenv

load_dotenv()

# MinIO
minio_client = Minio(
    os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)

# ClickHouse
# Подключаемся через HTTP (порт 8123)
ch_client = clickhouse_connect.get_client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
    username=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", "pass123"),
    database=os.getenv("CLICKHOUSE_DATABASE", "weather")
)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


@task(name="Получить прогноз погоды", retries=3, retry_delay_seconds=10)
def fetch_weather_data(cities: List[str]) -> Dict[str, Any]:
    """Получает прогноз на завтра для списка городов"""
    weather_data = {}
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    
    for city in cities:
        # Координаты городов (можно вынести в словарь)
        coords = {
            "Москва": {"lat": 55.7558, "lon": 37.6176},
            "Самара": {"lat": 53.2007, "lon": 50.15}
        }.get(city)
        
        if not coords:
            print(f"Координаты для {city} не найдены")
            continue

        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={coords['lat']}&longitude={coords['lon']}"
            f"&hourly=temperature_2m,precipitation,wind_speed_10m,wind_direction_10m"
            f"&forecast_days=2"
            f"&timezone=Europe/Moscow"
        )

        response = requests.get(url)
        if response.status_code == 200:
            weather_data[city] = response.json()
        else:
            raise Exception(f"Ошибка API для {city}: {response.status_code}")
    
    return weather_data


@task(name="Сохранить JSON в MinIO")
def save_raw_to_minio(weather_data: Dict[str, Any]):
    """Сохраняет сырые данные в MinIO как JSON"""
    bucket_name = os.getenv("MINIO_BUCKET")
    
    # Создать бакет, если не существует
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    for city, data in weather_data.items():
        object_name = f"raw/{city}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
        
        minio_client.put_object(
            bucket_name,
            object_name,
            data=io.BytesIO(json_bytes),
            length=len(json_bytes),
            content_type='application/json'
        )
        print(f"Сырые данные для {city} сохранены в MinIO: {object_name}")


@task(name="Нормализовать почасовые данные")
def transform_hourly_data(weather_data: Dict[str, Any]) -> List[Dict]:
    """Извлекает почасовые данные для таблицы weather_hourly"""
    hourly_records = []
    tomorrow = (datetime.now() + timedelta(days=1)).date()

    for city, data in weather_data.items():
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        precip = hourly.get("precipitation", [])
        wind_speed = hourly.get("wind_speed_10m", [])
        wind_dir = hourly.get("wind_direction_10m", [])

        for i, time_str in enumerate(times):
            time_dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            if time_dt.date() == tomorrow:
                record = {
                    "city": city,
                    "datetime": time_dt,
                    "temperature": temps[i] if i < len(temps) else None,
                    "precipitation": precip[i] if i < len(precip) else None,
                    "wind_speed": wind_speed[i] if i < len(wind_speed) else None,
                    "wind_direction": str(wind_dir[i]) if i < len(wind_dir) else None
                }
                hourly_records.append(record)
    
    return hourly_records


@task(name="Агрегировать дневные данные")
def transform_daily_data(weather_data: Dict[str, Any]) -> List[Dict]:
    """Считает min/max/avg температуру и осадки, добавляет предупреждения"""
    daily_records = []
    tomorrow = (datetime.now() + timedelta(days=1)).date()

    for city, data in weather_data.items():
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        precip = hourly.get("precipitation", [])
        wind_speed = hourly.get("wind_speed_10m", [])

        day_temps = []
        day_precip = 0.0
        max_wind = 0.0

        for i, time_str in enumerate(times):
            time_dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            if time_dt.date() == tomorrow:
                if i < len(temps): day_temps.append(temps[i])
                if i < len(precip): day_precip += precip[i]
                if i < len(wind_speed): max_wind = max(max_wind, wind_speed[i])

        if not day_temps:
            continue

        wind_alert = ""
        if max_wind > 15:
            wind_alert = "Сильный ветер!"
        if day_precip > 10:
            wind_alert += " Сильные осадки!" if wind_alert else "Сильные осадки!"

        record = {
            "city": city,
            "date": tomorrow,
            "temp_min": min(day_temps),
            "temp_max": max(day_temps),
            "temp_avg": sum(day_temps) / len(day_temps),
            "total_precipitation": day_precip,
            "wind_alert": wind_alert.strip()
        }
        daily_records.append(record)
    
    return daily_records

@task(name="Загрузить дневные данные в ClickHouse")
def load_daily_to_clickhouse(records: List[Dict]):
    if not records:
        print("Нет дневных данных для загрузки")
        return

    data = [
        (
            r["city"],
            r["date"],
            r["temp_min"],
            r["temp_max"],
            r["temp_avg"],
            r["total_precipitation"],
            r["wind_alert"]
        )
        for r in records
    ]

    ch_client.insert(
        table="weather.weather_daily",
        data=data,
        column_names=["city", "date", "temp_min", "temp_max", "temp_avg", "total_precipitation", "wind_alert"]
    )
    print(f"Загружено {len(records)} дневных записей в ClickHouse")


@task(name="Загрузить почасовые данные в ClickHouse")
def load_hourly_to_clickhouse(records: List[Dict]):
    if not records:
        print("Нет почасовых данных для загрузки")
        return

    # Преобразуем список словарей в список кортежей в правильном порядке
    data = [
        (
            r["city"],
            r["datetime"],
            r["temperature"],
            r["precipitation"],
            r["wind_speed"],
            r["wind_direction"]
        )
        for r in records
    ]

    ch_client.insert(
        table="weather.weather_hourly",
        data=data,
        column_names=["city", "datetime", "temperature", "precipitation", "wind_speed", "wind_direction"]
    )
    print(f"Загружено {len(records)} почасовых записей в ClickHouse")


@task(name="Отправить уведомление в Telegram")
def send_telegram_notification(daily_: List[Dict]):
    """Отправляет краткий прогноз и предупреждения в Telegram"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram не настроен — пропускаем отправку")
        return

    message = "Прогноз на завтра:\n\n"
    for record in daily_:
        alert = f"{record['wind_alert']}" if record['wind_alert'] else ""
        message += (
            f"{record['city']}:\n"
            f"  Темп: {record['temp_min']:.1f}°..{record['temp_max']:.1f}° (ср. {record['temp_avg']:.1f}°){alert}\n"
            f"  Осадки: {record['total_precipitation']:.1f} мм\n\n"
        )

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }

    response = requests.post(url, data=payload)
    if response.status_code == 200:
        print("Уведомление отправлено в Telegram")
    else:
        print(f"Ошибка отправки в Telegram: {response.text}")


@flow(name="weather_etl", log_prints=True)
def weather_etl(cities: List[str] = ["Москва", "Самара"]):
    """Основной ETL-пайплайн погоды"""
    print("Запуск ETL пайплайна погоды")

    # Extract
    weather_data = fetch_weather_data(cities)

    # Сохраняем сырые данные
    save_raw_to_minio(weather_data)

    # Transform
    hourly_data = transform_hourly_data(weather_data)
    daily_data = transform_daily_data(weather_data)

    # Load
    load_hourly_to_clickhouse(hourly_data)
    load_daily_to_clickhouse(daily_data)

    # Notify
    send_telegram_notification(daily_data)

    print("Пайплайн завершён успешно")

# Для тестирования чисто etl-пайплайна, без prefect
#if __name__ == "__main__":
#    weather_etl()