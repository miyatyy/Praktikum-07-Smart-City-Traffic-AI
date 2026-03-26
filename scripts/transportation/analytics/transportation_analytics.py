import pandas as pd
import glob
import os

def get_transport_metrics(data_path):
    """Fungsi untuk mengambil metrik dari file Parquet."""
    # Mencari semua file parquet di folder serving
    all_files = glob.glob(os.path.join(data_path, "*.parquet"))
    
    if not all_files:
        return None
    
    # Membaca semua file parquet menjadi satu DataFrame
    df = pd.concat((pd.read_parquet(f) for f in all_files), ignore_index=True)
    
    if df.empty:
        return None

    # Menghitung metrik utama
    metrics = {
        "total_trips": len(df),
        "total_fare": df['fare'].sum(),
        "avg_distance": df['distance'].mean(),
        "top_location": df['location'].mode()[0] if not df['location'].empty else "N/A",
        "data_raw": df
    }
    return metrics