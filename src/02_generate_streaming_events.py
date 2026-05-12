from pathlib import Path
from datetime import datetime
import argparse
import json
import random
import time

import pandas as pd
from confluent_kafka import Producer


# ============================================================
# CONFIGURACIÓN
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = BASE_DIR / "data" / "raw"

KAFKA_TOPIC = "netflix-events"

KAFKA_BOOTSTRAP_SERVERS = "broker:19092"


# ============================================================
# EVENTOS STREAMING
# ============================================================

EVENT_TYPES = [
    "play_started",
    "play_paused",
    "play_completed",
    "movie_liked",
    "subscription_upgraded",
    "subscription_cancelled"
]

EVENT_WEIGHTS = [
    0.35,
    0.15,
    0.25,
    0.10,
    0.10,
    0.05
]


# ============================================================
# CARGAR DATOS BASE
# ============================================================

def load_reference_data():

    users_df = pd.read_csv(RAW_DIR / "users.csv")

    movies_df = pd.read_csv(RAW_DIR / "movies.csv")

    subscriptions_df = pd.read_csv(RAW_DIR / "subscriptions.csv")

    return {
        "users_df": users_df,
        "movies_df": movies_df,
        "subscriptions_df": subscriptions_df
    }


# ============================================================
# CALLBACK KAFKA
# ============================================================

def delivery_report(err, msg):

    if err is not None:
        print(f"Error enviando mensaje: {err}")


# ============================================================
# CREAR EVENTO
# ============================================================

def create_event(event_number, reference_data):

    users_df = reference_data["users_df"]

    movies_df = reference_data["movies_df"]

    subscriptions_df = reference_data["subscriptions_df"]

    user = users_df.sample(1).iloc[0]

    movie = movies_df.sample(1).iloc[0]

    subscription = subscriptions_df[
        subscriptions_df["user_id"] == user["user_id"]
    ].iloc[0]

    event_type = random.choices(
        EVENT_TYPES,
        weights=EVENT_WEIGHTS,
        k=1
    )[0]

    watch_minutes = random.randint(1, movie["duration_minutes"])

    completion_rate = round(
        (watch_minutes / movie["duration_minutes"]) * 100,
        2
    )

    is_binge = watch_minutes >= 120

    event = {
        "event_id": f"EVT-{event_number:06d}",
        "user_id": user["user_id"],
        "country": user["country"],
        "subscription_plan": subscription["plan"],
        "movie_id": movie["movie_id"],
        "movie_title": movie["title"],
        "genre": movie["genre"],
        "event_type": event_type,
        "watch_minutes": watch_minutes,
        "completion_rate": completion_rate,
        "device": random.choice([
            "Mobile",
            "TV",
            "Laptop",
            "Tablet"
        ]),
        "is_binge": is_binge,
        "event_timestamp": datetime.now().isoformat(timespec="seconds")
    }

    return event


# ============================================================
# MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description="Netflix Streaming Event Producer"
    )

    parser.add_argument(
        "--events",
        type=int,
        default=500
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=0.1
    )

    args = parser.parse_args()

    print("=" * 70)
    print("Netflix Kafka Producer")
    print("=" * 70)

    reference_data = load_reference_data()

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
    })

    for event_number in range(1, args.events + 1):

        event = create_event(
            event_number,
            reference_data
        )

        message_value = json.dumps(
            event,
            ensure_ascii=False
        )

        producer.produce(
            topic=KAFKA_TOPIC,
            key=event["user_id"],
            value=message_value,
            callback=delivery_report
        )

        producer.poll(0)

        if event_number <= 5 or event_number % 100 == 0:
            print(f"Evento enviado: {message_value}")

        time.sleep(args.delay)

    producer.flush()

    print("=" * 70)
    print("Eventos enviados correctamente")
    print("=" * 70)


if __name__ == "__main__":
    main()