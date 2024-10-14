import requests
import json
import time
import os

class LastFM:
    BASE_URL = 'http://ws.audioscrobbler.com/2.0/'

    def __init__(self, config_file='lastfm-settings.json'):
        self.api_key, self.user = self.load_config(config_file)

    def load_config(self, config_file):
        """Load API key and username from a JSON file."""
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Config file '{config_file}' not found.")
        
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        api_key = config.get('key')
        user = config.get('user')
        
        if not api_key or not user:
            raise ValueError("API_KEY and username must be provided in the config file.")
        
        return api_key, user

    def get_recent_tracks(self, limit=200, page=1):
        """Get recent tracks from the Last.fm API."""
        url = f"{self.BASE_URL}?method=user.getRecentTracks&user={self.user}&api_key={self.api_key}&format=json&limit={limit}&page={page}"
        response = requests.get(url)
        return response.json()

    def extract_all_tracks(self, track_limit=None):
        """Extract up to 'track_limit' tracks, or all tracks if 'track_limit' is None."""
        page = 1
        all_tracks = []
        total_pages = 1  # Will be updated after the first request

        while page <= total_pages:
            data = self.get_recent_tracks(page=page)

            # Check for errors in response
            if 'error' in data:
                print(f"Error: {data['message']}")
                break

            # Get total pages from the first response
            if page == 1:
                total_pages = int(data['recenttracks']['@attr']['totalPages'])
                print(f"Total pages to fetch: {total_pages}")

            tracks = data['recenttracks']['track']
            all_tracks.extend(tracks)

            print(f"Fetched page {page}/{total_pages}, {len(all_tracks)}/{track_limit} tracks", end='\r')

            # Stop if we've collected enough tracks
            if track_limit and len(all_tracks) >= track_limit:
                all_tracks = all_tracks[:track_limit]  # Trim to exact limit
                break

            page += 1

            # Be respectful to the API's rate limit
            time.sleep(0.4)

        return all_tracks

    def save_tracks_to_file(self, tracks, output_file='data/lastfm_listening_history.json'):
        """Save extracted tracks to a JSON file."""
        with open(output_file, 'w') as f:
            json.dump(tracks, f, indent=4)
        print(f"Saved {len(tracks)} tracks to {output_file}")


# Example usage
if __name__ == '__main__':
    lastfm = LastFM(config_file='lastfm_config.json')
    
    # Specify the number of tracks you want to extract
    number_of_tracks_to_extract = 500  # Set to None to fetch all tracks
    
    all_tracks = lastfm.extract_all_tracks(track_limit=number_of_tracks_to_extract)
    lastfm.save_tracks_to_file(all_tracks)
