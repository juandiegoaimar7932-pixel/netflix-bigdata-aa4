import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Configuración de rutas
BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_DIR = BASE_DIR / "output" / "kpis"
CHARTS_DIR = BASE_DIR / "output" / "charts"

# Crear carpeta de gráficos si no existe
CHARTS_DIR.mkdir(parents=True, exist_ok=True)

def plot_most_watched_genres():
    print("Generando gráfico: Géneros más vistos...")
    df = pd.read_csv(INPUT_DIR / "most_watched_genres.csv")
    
    plt.figure(figsize=(10, 6))
    plt.bar(df['genre'], df['total_views'], color='skyblue')
    plt.title('Top 10 Géneros más vistos en Netflix')
    plt.xlabel('Género')
    plt.ylabel('Total de Visualizaciones')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / "most_watched_genres.png")
    plt.close()

def plot_subscriptions_by_plan():
    print("Generando gráfico: Distribución de Planes...")
    df = pd.read_csv(INPUT_DIR / "subscriptions_by_plan.csv")
    
    plt.figure(figsize=(8, 8))
    plt.pie(df['total_users'], labels=df['plan'], autopct='%1.1f%%', colors=['gold', 'lightcoral', 'lightgreen'])
    plt.title('Distribución de Planes de Suscripción')
    plt.savefig(CHARTS_DIR / "subscriptions_by_plan.png")
    plt.close()

def main():
    print("=" * 70)
    print("Generando Visualizaciones de Netflix")
    print("=" * 70)
    
    try:
        plot_most_watched_genres()
        plot_subscriptions_by_plan()
        print(f"\n¡Éxito! Gráficos guardados en: {CHARTS_DIR}")
    except FileNotFoundError:
        print("Error: No se encontraron los archivos CSV en output/kpis/. Ejecuta primero el script 03.")

    print("=" * 70)

if __name__ == "__main__":
    main()