from pathlib import Path
from datetime import datetime, timedelta
import random

import pandas as pd


# ============================================================
# CONFIGURACIÓN
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = BASE_DIR / "data" / "raw"

RAW_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# PARÁMETROS
# ============================================================

TOTAL_USERS = 1000
TOTAL_MOVIES = 300
TOTAL_WATCH_HISTORY = 15000


GENRES = [
    "Action",
    "Comedy",
    "Drama",
    "Sci-Fi",
    "Romance",
    "Horror",
    "Animation",
    "Documentary",
    "Thriller",
    "Fantasy"
]

SUBSCRIPTION_PLANS = [
    "Basic",
    "Standard",
    "Premium"
]

COUNTRIES = [
    "USA",
    "Canada",
    "Brazil",
    "Peru",
    "Mexico",
    "Spain",
    "Argentina",
    "Chile"
]


# ============================================================
# GENERAR GÉNEROS
# ============================================================

def generate_genres():
    rows = []

    for i, genre in enumerate(GENRES, start=1):
        rows.append({
            "genre_id": f"G{i:03d}",
            "genre_name": genre
        })

    return pd.DataFrame(rows)


# ============================================================
# GENERAR PELÍCULAS
# ============================================================

def generate_movies():
    rows = []

    for i in range(1, TOTAL_MOVIES + 1):

        release_year = random.randint(1990, 2025)

        rows.append({
            "movie_id": f"M{i:05d}",
            "title": f"Movie {i}",
            "genre": random.choice(GENRES),
            "release_year": release_year,
            "duration_minutes": random.randint(80, 180),
            "rating": round(random.uniform(2.5, 5.0), 1),
            "views": random.randint(1000, 500000)
        })

    return pd.DataFrame(rows)


# ============================================================
# GENERAR USUARIOS
# ============================================================

def generate_users():
    rows = []

    for i in range(1, TOTAL_USERS + 1):

        registration_date = (
            datetime.now()
            - timedelta(days=random.randint(30, 2000))
        ).date()

        rows.append({
            "user_id": f"U{i:05d}",
            "country": random.choice(COUNTRIES),
            "age": random.randint(18, 65),
            "subscription_plan": random.choice(SUBSCRIPTION_PLANS),
            "registration_date": registration_date
        })

    return pd.DataFrame(rows)


# ============================================================
# GENERAR SUSCRIPCIONES
# ============================================================

def generate_subscriptions(users_df):
    rows = []

    monthly_price_map = {
        "Basic": 8.99,
        "Standard": 12.99,
        "Premium": 17.99
    }

    for _, user in users_df.iterrows():

        plan = user["subscription_plan"]

        rows.append({
            "subscription_id": f"S{random.randint(100000,999999)}",
            "user_id": user["user_id"],
            "plan": plan,
            "monthly_price": monthly_price_map[plan],
            "is_active": random.choice([True, True, True, False])
        })

    return pd.DataFrame(rows)


# ============================================================
# GENERAR HISTORIAL DE VISUALIZACIÓN
# ============================================================

def generate_watch_history(users_df, movies_df):
    rows = []

    user_ids = users_df["user_id"].tolist()
    movie_ids = movies_df["movie_id"].tolist()

    for i in range(1, TOTAL_WATCH_HISTORY + 1):

        watch_time = (
            datetime.now()
            - timedelta(days=random.randint(0, 365))
        )

        duration_watched = random.randint(5, 180)

        rows.append({
            "watch_id": f"W{i:07d}",
            "user_id": random.choice(user_ids),
            "movie_id": random.choice(movie_ids),
            "watched_at": watch_time,
            "minutes_watched": duration_watched,
            "completed": random.choice([True, False]),
            "device": random.choice([
                "Mobile",
                "TV",
                "Laptop",
                "Tablet"
            ])
        })

    return pd.DataFrame(rows)


# ============================================================
# GUARDAR CSV
# ============================================================

def save_csv(df, filename):
    path = RAW_DIR / filename

    df.to_csv(path, index=False, encoding="utf-8")

    print(f"Archivo generado: data/raw/{filename}")


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("Generando datos batch de Netflix")
    print("=" * 70)

    genres_df = generate_genres()

    movies_df = generate_movies()

    users_df = generate_users()

    subscriptions_df = generate_subscriptions(users_df)

    watch_history_df = generate_watch_history(
        users_df,
        movies_df
    )

    save_csv(genres_df, "genres.csv")

    save_csv(movies_df, "movies.csv")

    save_csv(users_df, "users.csv")

    save_csv(subscriptions_df, "subscriptions.csv")

    save_csv(watch_history_df, "watch_history.csv")

    # --- AQUÍ LOS FORMATOS EXTRA PARA ASEGURAR LA NOTA ---
    
    # Guardar en JSON (Formato 2)
    movies_df.to_json(RAW_DIR / "movies.json", orient="records", indent=4)
    print(f"Archivo generado: data/raw/movies.json")

    print("=" * 70)
    print("Datos generados correctamente")
    print("=" * 70)


if __name__ == "__main__":
    main()