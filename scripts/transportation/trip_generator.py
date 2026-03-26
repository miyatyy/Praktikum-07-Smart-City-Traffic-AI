import json
import random
import time
import os
from datetime import datetime

# Konfigurasi Path - Mengarah ke folder stream_data yang tadi dibuat
OUTPUT_PATH = "../../stream_data/transportation"

# Pastikan folder tujuan ada
os.makedirs(OUTPUT_PATH, exist_ok=True)

def generate_trip_data():
    """Simulasi data perjalanan transportasi pintar."""
    vehicle_types = ['Car', 'Motorbike', 'Taxi']
    locations = ['Jakarta', 'Bandung', 'Surabaya']
    
    trip_id = f"TRIP-{int(time.time())}-{random.randint(1000, 9999)}"
    vehicle = random.choice(vehicle_types)
    location = random.choice(locations)
    distance = round(random.uniform(1.0, 50.0), 2)  # Jarak dalam KM
    
    # Logika tarif: Base fare + (jarak * per_km)
    base_fare = 10000 if vehicle == 'Car' else 5000
    fare = base_fare + (distance * 5000)
    
    # Simulasi Anomali (Tarif sangat tinggi untuk ditangkap Alert nanti)
    if random.random() < 0.1:  # 10% peluang data anomali
        fare = random.uniform(95000, 150000)

    return {
        "trip_id": trip_id,
        "vehicle_type": vehicle,
        "location": location,
        "distance": distance,
        "fare": int(fare),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def main():
    print(f"[*] Trip Generator AKTIF. Mengirim data ke: {OUTPUT_PATH}")
    print("[*] Tekan Ctrl+C untuk berhenti.")
    
    try:
        while True:
            trip = generate_trip_data()
            file_name = f"trip_{int(time.time() * 1000)}.json"
            file_path = os.path.join(OUTPUT_PATH, file_name)
            
            with open(file_path, 'w') as f:
                json.dump(trip, f)
            
            print(f"[SENT] {trip['trip_id']} | {trip['location']} | Rp{trip['fare']}")
            
            # Jeda waktu agar streaming terlihat natural
            time.sleep(random.uniform(1, 3))
            
    except KeyboardInterrupt:
        print("\n[*] Generator dimatikan.")

if __name__ == "__main__":
    main()