"""
This script cleans the raw listings and images before clip does
"""
import pandas as pd
import numpy as np
import matplotlib as mlpt

def data_clean(listing_data, images_data):
    """
    This function removes rows with missing values
    then removes listings with less than 5 images
    then removes images that were associated with removed listings
    """
    listing_data = listing_data.dropna()
    images_data = images_data.dropna()
    listing_data = listing_data[listing_data['image_count'] >= 5]
    images_data = images_data[images_data['source_id'].isin(listing_data['source_id'])]
    listing_data.apply(fix_floating, axis=1)

    return listing_data, images_data

#Function to replace floating apartments
def fix_floating(row):
    if row['floor_number'] > row['floors_total']:
        row['floors_total'] = row['floor_number'] * 2

    return row
