import fastf1
import pandas as pd
import os

# Setup path biar script bisa di-run dari mana aja
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
CACHE_DIR = os.path.join(DATA_DIR, 'f1_cache')

# Bikin folder kalau belum ada
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# Enable cache. Penting banget biar pas run ulang gak usah download API dari awal
fastf1.Cache.enable_cache(CACHE_DIR)

def fetch_and_clean_f1_data(seasons):
    # Tampungan sementara buat dataframe
    race_results_list = []
    quali_results_list = []
    
    for year in seasons:
        print(f"Fetching season {year}...")
        
        try:
            # Ambil jadwal balapan di tahun berjalan
            schedule = fastf1.get_event_schedule(year)
            
            # Ambil ronde reguler aja, abaikan ronde 0 (pre-season testing)
            rounds = schedule[schedule['RoundNumber'] > 0]['RoundNumber'].tolist()
            
            for round_num in rounds:
                print(f"  Round {round_num}...")
                
                # --- 1. Qualifying ---
                try:
                    quali_session = fastf1.get_session(year, round_num, 'Q')
                    
                    # Set False buat telemetry dkk biar cepet, soalnya cuma butuh hasil posisi
                    quali_session.load(telemetry=False, laps=False, weather=False)
                    
                    # Pastikan datanya gak kosong
                    if quali_session.results is not None and not quali_session.results.empty:
                        q_df = quali_session.results.copy()
                        
                        # Tambahin metadata
                        q_df['Season'] = year
                        q_df['Round'] = round_num
                        q_df['EventName'] = quali_session.event['EventName']
                        
                        quali_results_list.append(q_df)
                except Exception as e:
                    print(f"  [!] Gagal ambil Q {year}-{round_num}: {e}")
                
                # --- 2. Race ---
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
                    print(f"  [!] Gagal ambil R {year}-{round_num}: {e}")
                    
        except Exception as e:
            print(f"[!] Gagal load jadwal {year}: {e}")

    print("\nMerging data...")
    
    # Save Race Data
    if race_results_list:
        final_race_df = pd.concat(race_results_list, ignore_index=True)
        # Buang kolom yang semua isinya beneran kosong biar lebih rapi
        final_race_df = final_race_df.dropna(how='all', axis=1)
        
        race_csv_path = os.path.join(DATA_DIR, 'race_results.csv')
        final_race_df.to_csv(race_csv_path, index=False)
        print(f"Saved: {race_csv_path} ({len(final_race_df)} rows)")
        
    # Save Quali Data
    if quali_results_list:
        final_quali_df = pd.concat(quali_results_list, ignore_index=True)
        final_quali_df = final_quali_df.dropna(how='all', axis=1)
        
        quali_csv_path = os.path.join(DATA_DIR, 'qualifying_results.csv')
        final_quali_df.to_csv(quali_csv_path, index=False)
        print(f"Saved: {quali_csv_path} ({len(final_quali_df)} rows)")

if __name__ == "__main__":
    fetch_and_clean_f1_data([2024, 2025])
