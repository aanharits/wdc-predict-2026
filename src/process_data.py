import pandas as pd
import os

# Konfigurasi path utama script
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

def process_data():
    # Mengambil relasi dokumen data dasar
    quali_path = os.path.join(DATA_DIR, 'qualifying_results.csv')
    race_path = os.path.join(DATA_DIR, 'race_results.csv')
    
    if not os.path.exists(quali_path) or not os.path.exists(race_path):
        print("[!] File referensi tidak ditemukan. Harap jalankan modul fetch_data.py terlebih dahulu.")
        return
        
    df_quali = pd.read_csv(quali_path)
    df_race = pd.read_csv(race_path)
        
    # --- Merging Data (qualifying & race) ---
    df_merged = pd.merge(
        df_quali, 
        df_race, 
        on=['Season', 'Round', 'DriverNumber'], 
        suffixes=('_quali', '_race'), 
        how='inner' 
    )
    
    # Standarisasi referensi nama kolom variabel target 
    driver_col = 'Abbreviation_race' if 'Abbreviation_race' in df_merged.columns else 'Abbreviation'
    team_col = 'TeamName_race' if 'TeamName_race' in df_merged.columns else 'TeamName'
    
    # Mengamankan nilai Grid Position awal (fallback ke nilai uji klasifikasi bila absen)
    grid_col = 'GridPosition_race' if 'GridPosition_race' in df_merged.columns else 'GridPosition'
    if grid_col not in df_merged.columns:
        grid_col = 'Position_quali' if 'Position_quali' in df_merged.columns else 'Position'
        
    final_pos_col = 'Position_race' if 'Position_race' in df_merged.columns else 'Position'

    # Mapping target atribut
    df_merged['DriverAbbreviation'] = df_merged[driver_col]
    df_merged['TeamName'] = df_merged[team_col]
    df_merged['GridPosition'] = df_merged[grid_col]
    df_merged['FinalPosition'] = df_merged[final_pos_col]
    
    # --- Data Cleaning ---;
    # Konversi result atribut posisi dari tipe String menjadi Integer/Numerik.
    df_merged['FinalPosition'] = pd.to_numeric(df_merged['FinalPosition'], errors='coerce')
    df_merged['GridPosition'] = pd.to_numeric(df_merged['GridPosition'], errors='coerce')
    
    # Menggunakan metode imputasi di mana tipe record NaN (DNS, DNF, absen, dsb) didefault ke urutan 20.
    df_merged['FinalPosition'] = df_merged['FinalPosition'].fillna(20).astype(int)
    df_merged['GridPosition'] = df_merged['GridPosition'].fillna(20).astype(int)
    
    # --- Finalisasi Data ---
    # Finalisasi seleksi variabel untuk penggunaan model Machine Learning
    output_columns = ['Season', 'Round', 'DriverAbbreviation', 'TeamName', 'GridPosition', 'FinalPosition']
    df_final = df_merged[output_columns]
    
    output_path = os.path.join(DATA_DIR, 'f1_processed.csv')
    df_final.to_csv(output_path, index=False)
    
    print(f"Data diproses: {output_path} (Terdapat {len(df_final)} observasi)\n")
    print("Pratinjau atribut yang dihasilkan:")
    print(df_final.head())

if __name__ == "__main__":
    process_data()
