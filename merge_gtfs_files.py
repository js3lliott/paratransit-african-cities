import os
import pandas as pd
import gtfs_kit as gk

def load_gtfs_files(gtfs_folder_path):
    """Load GTFS feed from a folder containing unzipped GTFS text files."""
    return gk.read_feed(gtfs_folder_path, dist_units='km')

def create_unique_ids(feed, city_id):
    """Prefixes unique identifiers (e.g., stop_id, trip_id) with a city_id to ensure no conflicts."""
    for table in ['stops', 'trips', 'routes', 'shapes', 'stop_times']:
        if table in feed.tables:
            feed.tables[table] = feed.tables[table].apply(lambda col: col.astype(str) if col.dtype == object else col)
            feed.tables[table] = feed.tables[table].assign(**{
                col: city_id + "_" + feed.tables[table][col].astype(str) for col in feed.tables[table] if 'id' in col
            })
    return feed

def merge_feeds(feeds):
    """Merge multiple GTFS feeds."""
    merged_feed = {}
    for table in ['stops', 'trips', 'routes', 'shapes', 'stop_times', 'agency', 'calendar', 'frequencies']:
        # concatenate the tables from each feed
        merged_feed[table] = pd.concat([feed.tables[table] for feed in feeds if table in feed.tables], ignore_index=True)
    return merged_feed

def save_merged_feed(merged_feed, output_path):
    """Saves the merged GTFS feed to a zip file or folder"""
    gk.write_feed(gk.Feed(merged_feed), output_path)

gtfs_dirs = [
    './data/raw/Abidjan',
    './data/raw/Accra',
    './data/raw/Addis',
    './data/raw/Freetown',
    './data/raw/Harare',
    './data/raw/Kampala',
    './data/raw/Nairobi'
]

# Load GTFS data and create unique identifiers for each feed
feeds = [create_unique_ids(load_gtfs_files(gtfs_dirs), os.path.basename(gtfs_dirs)) for gtfs_dir in gtfs_dirs]

# Merge the feeds
merged_feed = merge_feeds(feeds)

# save the merged feed to a new folder or zip file
save_merged_feed(merged_feed, './data/processed/merged_gtfs')