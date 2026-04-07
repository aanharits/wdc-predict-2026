import fastf1
import pandas as pd
import os

# ==========================================
# Konfigurasi Direktori
# ==========================================
# Menggunakan absolute path relatif terhadap letak script 
# agar aman dijalankan dari folder manapun
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
CACHE_DIR = os.path.join(DATA_DIR, 'f1_cache')

# Memastikan folder direktori ada sebelum script mulai mengunduh data
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# Mengaktifkan cache penyimpanan fastf1.
# Penggunaan cache sangat krusial dalam library fastf1 agar API tidak
# mendownload data yang sama berulang kali saat script dijalankan ulang.
fastf1.Cache.enable_cache(CACHE_DIR)

def fetch_and_clean_f1_data(seasons):
    """
    Fungsi utama untuk mengambil dan melakukan pembersihan data dasar hasil 
    Balapan dan Kualifikasi pada rentang musim yang ditentukan.
    Hasil akhirnya disatukan ke dalam satu file CSV gabungan per jenis sesi.
    """
    race_results_list = []
    quali_results_list = []
    
    for year in seasons:
        print(f"==========================================")
        print(f"Memulai pengambilan data musim {year}...")
        print(f"==========================================")
        
        try:
            # Mengambil jadwal dari seluruh rangkaian event resmi untuk musim `year`
            schedule = fastf1.get_event_schedule(year)
            
            # Memfilter hanya ronde reguler (RoundNumber > 0).
            # Fungsi ini otomatis akan mengabaikan pre-season testing yang berindeks 0
            rounds = schedule[schedule['RoundNumber'] > 0]['RoundNumber'].tolist()
            
            for round_num in rounds:
                print(f"Processing Musim {year} - Ronde {round_num}...")
                
                # ----------------------------------------------------
                # 1. Mengambil Data Kualifikasi ('Q' - Qualifying)
                # ----------------------------------------------------
                try:
                    # Ambil object sesi Kualifikasi
                    quali_session = fastf1.get_session(year, round_num, 'Q')
                    
                    # Load data. Argumen telemetry, laps, dan weather 
                    # di-set False agar proses download menjadi jauh lebih cepat
                    # karena kita hanya peduli pada posisi akhir pembalap (Results).
                    quali_session.load(telemetry=False, laps=False, weather=False)
                    
                    # Validasi jika datanya ada
                    if quali_session.results is not None and not quali_session.results.empty:
                        q_df = quali_session.results.copy()
                        
                        # Menambahkan kolom penanda dari musim & nama event terkait
                        q_df['Season'] = year
                        q_df['Round'] = round_num
                        q_df['EventName'] = quali_session.event['EventName']
                        
                        quali_results_list.append(q_df)
                except Exception as e:
                    print(f" -> [WARNING] Gagal mengambil data kualifikasi musim {year} ronde {round_num}: {e}")
                
                # ----------------------------------------------------
                # 2. Mengambil Data Balapan Utama ('R' - Race)
                # ----------------------------------------------------
                try:
                    # Ambil object sesi Balapan Utama
                    race_session = fastf1.get_session(year, round_num, 'R')
                    race_session.load(telemetry=False, laps=False, weather=False)
                    
                    if race_session.results is not None and not race_session.results.empty:
                        r_df = race_session.results.copy()
                        
                        r_df['Season'] = year
                        r_df['Round'] = round_num
                        r_df['EventName'] = race_session.event['EventName']
                        
                        race_results_list.append(r_df)
                except Exception as e:
                    print(f" -> [WARNING] Gagal mengambil data balapan musim {year} ronde {round_num}: {e}")
                    
        except Exception as e:
            print(f" -> [ERROR] Gagal memuat jadwal musim {year}: {e}")

    # ==========================================
    # Tahap Pembersihan Dasar & Penyimpanan Data
    # ==========================================
    print("\nProses pengambilan selesai. Menyatukan frame data...")
    
    # Memproses Data Balapan
    if race_results_list:
        final_race_df = pd.concat(race_results_list, ignore_index=True)
        # Pembersihan Data: Membuang baris atau kolom yang keseluruhan nilainya NA
        final_race_df = final_race_df.dropna(how='all', axis=1)
        
        race_csv_path = os.path.join(DATA_DIR, 'race_results.csv')
        final_race_df.to_csv(race_csv_path, index=False)
        print(f"[SUCCESS] Data Balapan disimpan ke {race_csv_path} ({len(final_race_df)} baris)")
    else:
        print("[INFO] Tidak ada data Balapan yang berhasil diproses.")
        
    # Memproses Data Kualifikasi
    if quali_results_list:
        final_quali_df = pd.concat(quali_results_list, ignore_index=True)
        final_quali_df = final_quali_df.dropna(how='all', axis=1)
        
        quali_csv_path = os.path.join(DATA_DIR, 'qualifying_results.csv')
        final_quali_df.to_csv(quali_csv_path, index=False)
        print(f"[SUCCESS] Data Kualifikasi disimpan ke {quali_csv_path} ({len(final_quali_df)} baris)")
    else:
        print("[INFO] Tidak ada data Kualifikasi yang berhasil diproses.")

if __name__ == "__main__":
    # Menentukan musim balapan mana saja yang akan diambil datanya (2024 & 2025)
    SEASONS_TO_FETCH = [2024, 2025]
    fetch_and_clean_f1_data(SEASONS_TO_FETCH)
