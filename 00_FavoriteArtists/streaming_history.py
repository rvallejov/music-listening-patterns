import time
import pandas as pd
from datetime import datetime

from lastfm import LastFM
from spotify import Spotify

today_date = datetime.today().strftime('%Y_%m_%d')

class StreamETL:
    def get_streams(self, track_limit=None):

        # Load Last.fm credentials
        lastfm = LastFM(config_file='lastfm-settings.json')

        # Extract recent tracks from Last.fm
        lastfm_tracks = lastfm.extract_all_tracks(track_limit=track_limit)
        print(f"Successfully extracted {len(lastfm_tracks)} tracks from Last.fm.")
        
        # List to hold the data for CSV export
        data = []

        # Loop through each track and get the audio features from Spotify
        print("Processing tracks to fetch audio features from Spotify...")
        for index, track in enumerate(lastfm_tracks, start=1):
            stream_date = track['date']['#text'] if 'date' in track else 'now playing'
            track_name = track['name']
            artist_name = track['artist']['#text']
            album_name = track['album']['#text'] if 'album' in track else ""
            
            # Combine track information and audio features
            track_data = {
                'stream_date': stream_date,
                'track': track_name,
                'artist': artist_name,
                'album': album_name
            }
            
            # if get_audio_features:
            #     print(f"[{index}/{len(lastfm_tracks)}] Processing track: {track_name} by {artist_name}...", end='\r')

            #     # Fetch audio features from Spotify
            #     audio_features = spotify.get_track_audio_features(artist_name, album_name, track_name)

            #     if audio_features:
            #         # Add all the audio features to the dictionary
            #         track_data.update(audio_features)

            #     # Be respectful to the API's rate limit
            #     time.sleep(0.2)
            
            data.append(track_data)

            #print(f"Processed {len(data)} tracks with available audio features.")

        # Convert the list of dictionaries to a pandas DataFrame
        df = pd.DataFrame(data)

        # Save the data to a CSV file
        path = f'data/bronze/{today_date}_lastfm_streams.csv'
        df.to_csv(path, index=False)
        print(f"Saved {len(df)} tracks with audio features to {path}")

    def get_audio_features(self, df):

        # Initialize Spotify from the 'spotify-settings.json' file
        spotify = Spotify(config_file='spotify-settings.json')

        audio_features_list = []

        for index, row in df.iterrows():
            artist_name = row['artist']
            album_name = row['album']
            track_name = row['track']

            print(f"[{index + 1}/{len(df)}] Fetching audio features for track: {track_name} by {artist_name}...", end='\r')

            # Fetch audio features from Spotify
            audio_features = spotify.get_track_audio_features(artist_name, album_name, track_name)

            if audio_features:
                audio_features_list.append(audio_features)
            else:
                audio_features_list.append({})

            # Be respectful to the API's rate limit
            time.sleep(0.2)

        # Convert the list of dictionaries to a pandas DataFrame
        audio_features_df = pd.DataFrame(audio_features_list)

        # Concatenate the original dataframe with the audio features dataframe
        result_df = pd.concat([df.reset_index(drop=True), audio_features_df.reset_index(drop=True)], axis=1)

        # Save the data to a CSV file
        path = f'data/bronze/{today_date}_spotify_audio_features.csv'
        result_df.to_csv(path, index=False)
        print(f"Saved {len(result_df)} tracks with updated audio features to {path}")

    def clean_streams(self, df):
        df = df.query("stream_date != 'now playing'").copy()
        # Convert the 'stream_date' column to datetime format for proper grouping
        df['stream_date'] = pd.to_datetime(df['stream_date'], format='%d %b %Y, %H:%M')
        df['stream_month'] = df.stream_date - pd.to_timedelta(df.stream_date.dt.day - 1, unit = 'd') # may need to add '.dt' before '.day'
        df['stream_quarter'] = df.stream_date.dt.to_period('Q').apply(lambda x: pd.to_datetime(x.start_time))
        df['stream_year'] = df.stream_date.dt.year

        # Get top artists to get cleaner visualizations
        top_artists = df.groupby('artist').size().nlargest(500).index.tolist()
        df['artist_clean'] = df.artist.apply(lambda x: x if x in top_artists else 'Other')

        today_date = datetime.today().strftime('%Y_%m_%d')
        # Save the data to a CSV file
        path = f'data/silver/{today_date}_lastfm_streams.csv'
        df.to_csv(path, index=False)
        print(f"Saved {len(df)} tracks to {path}")

    def aggregate_data(self, df):
        # Group by date (stream_date) and artist, then count the number of rows per group
        grouped_df = df.groupby([df['stream_date'].dt.date, 'artist_clean']).size().reset_index(name='play_count')
        grouped_df['stream_date'] = pd.to_datetime(grouped_df['stream_date'])
        grouped_df['stream_month'] = grouped_df.stream_date - pd.to_timedelta(grouped_df.stream_date.dt.day - 1, unit = 'd') # may need to add '.dt' before '.day'
        grouped_df['cumulative_play_count'] = grouped_df.sort_values('stream_date').groupby('artist_clean').play_count.cumsum()

        first_stream_date = grouped_df.groupby('artist_clean').stream_date.min().reset_index()
        first_stream_date = dict(zip(first_stream_date.artist_clean, first_stream_date.stream_date))

        grouped_df['first_stream_date'] = grouped_df.artist_clean.map(first_stream_date)
        # Compute the number of days between first_stream_date and stream_date
        grouped_df['days_since_first_stream'] = (grouped_df['stream_date'] - grouped_df['first_stream_date']).dt.days

        path = f'data/gold/{today_date}_aggregate_streams.csv'
        grouped_df.to_csv(path, index=False)
        print(f"Saved {len(grouped_df)} rows to {path}")
