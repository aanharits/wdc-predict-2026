import fastf1
import pandas as pd
import os

# Konfigurasi path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
CACHE_DIR = os.path.join(DATA_DIR, 'f1_cache')

# Make Sure folder data tersedia
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# Enable cache fastf1 
fastf1.Cache.enable_cache(CACHE_DIR)

def fetch_and_clean_f1_data(seasons):
    # Array buat nampung result data
    race_results_list = []
    quali_results_list = []
    
    for year in seasons:
        print(f"Mengambil data musim {year}...")
        
        try:
            # Mengambil data jadwal kompetisi
            schedule = fastf1.get_event_schedule(year)
            
            # Memilih seri resmi kompetisi (Indeks Ronde > 0 mengabaikan event uji coba)
            rounds = schedule[schedule['RoundNumber'] > 0]['RoundNumber'].tolist()
            
            for round_num in rounds:
                print(f"  Memproses Ronde {round_num}...")
                
                # --- 1. Data Qualifying ---
                try:
                    quali_session = fastf1.get_session(year, round_num, 'Q')
                    quali_session.load(telemetry=False, laps=False, weather=False)
                    
                    if quali_session.results is not None and not quali_session.results.empty:
                        q_df = quali_session.results.copy()
                        
                        # Menambahkan kolom referensi event
                        q_df['Season'] = year
                        q_df['Round'] = round_num
                        q_df['EventName'] = quali_session.event['EventName']
                        
                        quali_results_list.append(q_df)
                except Exception as e:
                    print(f"  [!] Gagal memuat Kualifikasi {year}-{round_num}: {e}")
                
                # --- 2. Data Race ---
                try:
                    race_session = fastf1.get_session(year, round_num, 'R')
                    race_session.load(telemetry=False, laps=False, weather=False)
                    
                    if race_session.results is not None and not race_session.results.empty:
                        r_df = race_session.results.copy()
                        
                        r_df['Season'] = year
                        r_df['Round'] = round_num
                        r_df['EventName'] = race_session.event['EventName']
                        
                        race_results_list.append(r_df)
                except Exception as e:
                    print(f"  [!] Gagal memuat Balapan {year}-{round_num}: {e}")
                    
        except Exception as e:
            print(f"[!] Gagal menemukan jadwal di musim {year}: {e}")

    print("\nMengkonsolidasi struktur array...")
    
    # Export Raw Data Race Result
    if race_results_list:
        final_race_df = pd.concat(race_results_list, ignore_index=True)
        final_race_df = final_race_df.dropna(how='all', axis=1)
        
        race_csv_path = os.path.join(DATA_DIR, 'race_results.csv')
        final_race_df.to_csv(race_csv_path, index=False)
        print(f"Data balapan disimpan ke: {race_csv_path} ({len(final_race_df)} baris)")
        
    # Export Raw Data Qualifying Result
    if quali_results_list:
        final_quali_df = pd.concat(quali_results_list, ignore_index=True)
        final_quali_df = final_quali_df.dropna(how='all', axis=1)
        
        quali_csv_path = os.path.join(DATA_DIR, 'qualifying_results.csv')
        final_quali_df.to_csv(quali_csv_path, index=False)
        print(f"Data kualifikasi disimpan ke: {quali_csv_path} ({len(final_quali_df)} baris)")

if __name__ == "__main__":
    fetch_and_clean_f1_data([2024, 2025])
