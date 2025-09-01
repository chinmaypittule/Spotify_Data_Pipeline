import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
from airflow.models import TaskInstance
import os

from utils.constants import CLIENT_ID, SECRET, EXTRACT_PATH
client_id = CLIENT_ID
client_secret = SECRET

client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

def fetch_artist_albums(artist_id):
    albums = sp.artist_albums(artist_id=artist_id, album_type='album', limit=1)
    return albums['items']

def fetch_album_tracks(album_id):
    return sp.album_tracks(album_id=album_id)

def get_tracks_for_artist(unp_csv,ti: TaskInstance): 
    #ti = kwargs['ti']
    final_csv = EXTRACT_PATH
    df = pd.read_csv(unp_csv)
    if df.empty:
        raise ValueError(f"CSV file {unp_csv} is empty!")
    artist_id  = df.iloc[0,0]    
    
    all_tracks = []
    
    albums = fetch_artist_albums(artist_id)
        
    for album in albums:
        album_id = album['id']
        album_name = album['name']
        album_tracks = fetch_album_tracks(album_id)
            
        for track in album_tracks['items']:
            track_details = sp.track(track['id'])
            track_data = {
                "track_name": track['name'],
                "artist_id": artist_id,
                "artist_name": track_details['artists'][0]['name'],
                "track_id": track['id'],
                "album_name": album_name,
                "duration_ms": track['duration_ms'],
                "popularity": track_details['popularity'],
                "preview_url": track['preview_url'],
            }
            all_tracks.append(track_data)

    #ti.xcom_push(key='tracks_data', value=all_tracks)
    df_tracks = pd.DataFrame(all_tracks)
    if not os.path.exists(final_csv) or os.stat(final_csv).st_size == 0:
        df_tracks.to_csv(final_csv, mode='w', index=False, header=True)  # Add header if empty
    else:
        df_tracks.to_csv(final_csv, mode='a', index=False, header=False)
    #df_tracks.to_csv('/opt/airflow/dags/raw_extract.csv', mode='a', index=False, header=False)                      
    return all_tracks