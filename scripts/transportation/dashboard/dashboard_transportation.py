import streamlit as st
import pandas as pd
import os
import sys
import time

# Tambahkan path agar bisa mengenali folder analytics
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from analytics.transportation_analytics import get_transport_metrics

st.set_page_config(page_title="Smart Transportation Dashboard", layout="wide")

# Path ke data hasil streaming
DATA_PATH = "../../../data/serving/transportation"

st.title("🚗 Smart Transportation Real-Time Dashboard")
st.markdown("---")

# Placeholder untuk update data secara live
placeholder = st.empty()

while True:
    with placeholder.container():
        metrics = get_transport_metrics(DATA_PATH)
        
        if metrics:
            # 1. Barisan Metrik (KPI)
            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            kpi1.metric("Total Perjalanan", f"📈 {metrics['total_trips']}")
            kpi2.metric("Total Pendapatan", f"Rp {metrics['total_fare']:,}")
            kpi3.metric("Rata-rata Jarak", f"{metrics['avg_distance']:.2f} KM")
            kpi4.metric("Kota Teraktif", metrics["top_location"])

            df = metrics["data_raw"]

            # 2. Grafik Visualisasi
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("📊 Pendapatan per Lokasi")
                st.bar_chart(df.groupby("location")["fare"].sum())
            
            with col2:
                st.subheader("🚲 Distribusi Kendaraan")
                vehicle_count = df["vehicle_type"].value_counts()
                st.bar_chart(vehicle_count)

            # 3. Decision-Oriented Alert System
            st.subheader("🚨 System Alerts (Decision Support)")
            
            # Alert 1: Anomali Tarif
            anomalies = df[df['fare'] > 90000]
            if not anomalies.empty:
                st.error(f"ATTENTION: Terdeteksi {len(anomalies)} transaksi dengan tarif ekstrem (> Rp 90.000)!")
                st.dataframe(anomalies.tail(3))
            
            # Alert 2: Volume Traffic
            if metrics["total_trips"] > 100:
                st.warning("TRAFFIC ALERT: Volume perjalanan sangat tinggi. Pertimbangkan penambahan armada!")

            # 4. Tabel Data Terkini
            st.subheader("📋 Live Trip Logs")
            st.dataframe(df.sort_values(by="timestamp", ascending=False).head(10), use_container_width=True)
            
        else:
            st.info("Sedang menunggu data masuk dari Spark Streaming...")
            
    time.sleep(5) # Auto-refresh setiap 5 detik