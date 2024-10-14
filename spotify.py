import os
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

class Spotify:
    def __init__(self, config_file='spotify-settings.json'):
        """Initialize Spotify client using credentials from a JSON config file."""
        self.client_id, self.client_secret = self.load_config(config_file)
        self.spotify = self.authenticate()

    def load_config(self, config_file='spotify-settings.json'):
        """Load Spotify client_id and client_secret from a JSON file."""
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Config file '{config_file}' not found.")
        
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        client_id = config.get('clientid')
        client_secret = config.get('secret')

        if not client_id or not client_secret:
            raise ValueError("client_id and client_secret must be provided in the config file.")
        
        return client_id, client_secret

    def authenticate(self):
        """Authenticate the Spotify client using the client credentials."""
        auth_manager = SpotifyClientCredentials(client_id=self.client_id, client_secret=self.client_secret)
        return spotipy.Spotify(auth_manager=auth_manager)

    def get_track_audio_features(self, artist, album, track_name):
        """Search for a track on Spotify and get its audio features."""
        query = f"artist:{artist} track:{track_name} album:{album}"
        results = self.spotify.search(q=query, type='track', limit=1)

        if results['tracks']['items']:
            track_id = results['tracks']['items'][0]['id']
            features = self.spotify.audio_features([track_id])
            if features:
                return features[0]  # Return the first match's features
        return None
