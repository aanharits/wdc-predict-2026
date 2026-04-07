import pandas as pd
import os

# Setup Path direktori utama
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

def process_data():
    # 1. Membaca kedua file CSV hasil fetch sebelumnya
    quali_path = os.path.join(DATA_DIR, 'qualifying_results.csv')
    race_path = os.path.join(DATA_DIR, 'race_results.csv')
    
    # Validasi kecil kalau data mentahnya benar-benar ada
    if not os.path.exists(quali_path) or not os.path.exists(race_path):
        print("[!] File CSV belum lengkap. Harap jalankan fetch_data.py terlebih dahulu.")
        return
        
    df_quali = pd.read_csv(quali_path)
    df_race = pd.read_csv(race_path)
        
    # =========================================================================
    # 2. Menggabungkan (Merge) Data
    # -------------------------------------------------------------------------
    # Penjelasan Singkat tentang Merging di Pandas:
    # Merging adalah proses penggabungan dua kolom data (Dataset/DataFrame)
    # menjadi satu kesatuan berdasarkan kesamaan nilai di satu/banyak kolom 
    # acuan (disebut 'key columns'). 
    # 
    # Ini konsepnya mirip dengan "INNER JOIN" pada basis data SQL atau 
    # kolaborasi VLOOKUP dan MATCH di Excel.
    # 
    # Pada script ini, kita menggabungkan hasil kualifikasi ('df_quali') dan 
    # hasil balapan ('df_race') dengan melihat Season, Round, dan DriverNumber
    # agar data pembalap spesifik di sesi yang tepat tersambung jadi satu baris.
    # =========================================================================
    
    df_merged = pd.merge(
        df_quali, 
        df_race, 
        on=['Season', 'Round', 'DriverNumber'], 
        suffixes=('_quali', '_race'), # Tambahkan akhiran jika nama kolom sama (contoh: Position)
        how='inner' # Hanya pembalap yang ikut di kedua sesi (kualifikasi dan balapan)
    )
    
    # Kita cari perwakilan kolom untuk format target yang rapih karena setelah 
    # di-merge sangat mungkin menghasilkan kolom _quali / _race
    driver_col = 'Abbreviation_race' if 'Abbreviation_race' in df_merged.columns else 'Abbreviation'
    team_col = 'TeamName_race' if 'TeamName_race' in df_merged.columns else 'TeamName'
    
    # Ambil posisi start dari kolom GridPosition_race (jika ada), kalau tidak ada ambil hasil kualifikasinya
    grid_col = 'GridPosition_race' if 'GridPosition_race' in df_merged.columns else 'GridPosition'
    if grid_col not in df_merged.columns:
        grid_col = 'Position_quali' if 'Position_quali' in df_merged.columns else 'Position'
        
    final_pos_col = 'Position_race' if 'Position_race' in df_merged.columns else 'Position'

    # Buat format baku ke kolom yang sesuai dengan request (Sesuai output target)
    df_merged['DriverAbbreviation'] = df_merged[driver_col]
    df_merged['TeamName'] = df_merged[team_col]
    df_merged['GridPosition'] = df_merged[grid_col]
    df_merged['FinalPosition'] = df_merged[final_pos_col]
    
    # -------------------------------------------------------------------------
    # 3. Membersihkan kolom 'Position' agar berisi Numerik
    # -------------------------------------------------------------------------
    # Mengkonversi FinalPosition dan GridPosition agar format pandasnya jadi angka riil
    # `errors='coerce'` bakal maksa hal non-angka misal teks ("DNF", "NC") diubah jadi NaN.
    df_merged['FinalPosition'] = pd.to_numeric(df_merged['FinalPosition'], errors='coerce')
    df_merged['GridPosition'] = pd.to_numeric(df_merged['GridPosition'], errors='coerce')
    
    # Buat yang statusnya NaN tadi, kita isi default posisi terakhir asumsi 20.
    df_merged['FinalPosition'] = df_merged['FinalPosition'].fillna(20).astype(int)
    df_merged['GridPosition'] = df_merged['GridPosition'].fillna(20).astype(int)
    
    # -------------------------------------------------------------------------
    # 4. Menyaring / Memilih Kolom & Export ke CSV
    # -------------------------------------------------------------------------
    # Target Akhir Sesuai Request: Season, Round, DriverAbbreviation, TeamName, GridPosition, FinalPosition
    output_columns = ['Season', 'Round', 'DriverAbbreviation', 'TeamName', 'GridPosition', 'FinalPosition']
    df_final = df_merged[output_columns]
    
    # Simpan hasil olahan ini
    output_path = os.path.join(DATA_DIR, 'f1_processed.csv')
    df_final.to_csv(output_path, index=False)
    
    print(f"[SUCCESS] Proses data selesai! Berhasil disimpan di: {output_path}")
    print(f"Total baris data: {len(df_final)}\n")
    print("Contoh 5 data pertama hasil akhir yang telah diberishkan:")
    print(df_final.head())

if __name__ == "__main__":
    process_data()
