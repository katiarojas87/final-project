"""
This script cleans the raw listings and images and adds tags to images using images
"""
import pathlib
import pandas as pd
import numpy as np
import matplotlib as mlpt
import torch
from transformers import pipeline
import re

_layout_re = re.compile(r"^\s*(\d+)\s*(S?)\s*(LDK|DK|K|R)\s*$", re.IGNORECASE)

_fw_digits = str.maketrans("０１２３４５６７８９", "0123456789")

def parse_layout(X):
    """
    Input: X as 2D array (n_samples, 1) containing layout strings
    Output: DataFrame with columns:
      - rooms_num (int)
      - base_layout (str: LDK/DK/K/R)
      - has_S (int 0/1)
    """
    s = pd.Series(np.asarray(X).ravel()).fillna("").astype(str)
    s = s.str.translate(_fw_digits).str.strip().str.upper()

    rooms = []
    base = []
    has_s = []

    for val in s:
        m = _layout_re.match(val)
        if not m:
            rooms.append(np.nan)
            base.append("UNKNOWN")
            has_s.append(0)
        else:
            rooms.append(int(m.group(1)))
            has_s.append(1 if m.group(2) == "S" else 0)
            base.append(m.group(3))  # LDK/DK/K/R

    return pd.DataFrame({
        "rooms_num": rooms,
        "base_layout": base,
        "has_S": has_s
    })

def initialize_clip():
    clip = pipeline(
        task = "zero-shot-image-classification",
        model = "openai/clip-vit-base-patch32",
        dtype = torch.bfloat16,
        device=0
    )
    return clip

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

    layout = listing_data['layout']
    layout_parsed = parse_layout(layout)
    layout_parsed['has_S'].value_counts()
    layout_parsed = layout_parsed.drop(['has_S'], axis= 1)
    listing_data = listing_data.reset_index().join(layout_parsed).drop(['layout'], axis=1)

    return listing_data, images_data

#Function to replace floating apartments
def fix_floating(row):
    if row['floor_number'] > row['floors_total']:
        row['floors_total'] = row['floor_number'] * 2

    return row


def identify_default_images(image_path: str, clip):
    """
    Use CLIP to identify default images.
    Default images are computer generated images.
    Return 1 or 0.

    Input: image path
    Output: 1 if DefaultImage, 0 if not DefaultImage
    """

    try:
        labels = ["illustration, logo or advert", "floor plan", "image"]
        results = clip(image_path, candidate_labels=labels)

        if results[0]["label"] == labels[0]:
            default_image = 2
        elif results[0]["label"] == labels[1]:
            default_image = 1
        else:
            default_image = 0

    except:
        default_image = 2

    return default_image


def assign_room_type(image_path: str, labels: list, clip):
    """
    Use CLIP to identify the room type of the image.
    Results are list of dictionaries, automatically sorted by decreasing score.
    Return label ob first entry in results.

    Input: image path and list of possible room types
    Output: label of room type depicted in the image
    """
    results = clip(image_path, candidate_labels=labels)
    room_type = results[0]["label"]
    score = results[0]["score"]

    return room_type


def get_score(image_path: str, room_type: str, attribute_list: list, clip):
    """
    Use CLIP to score the room according to each attribute.
    The list of attributes depends on the room type according to the attribute_dict.
    Returns dictionary with attributes as keys and scores as values.

    Input: image path and list of possible room types
    Output: label of room type depicted in the image
    """

    dict = {}

    for attribute in attribute_list:
        if room_type == "floor plan":
            dict[attribute] = -1000

        else:
            if attribute == "brightness":
                labels = ["bright", "not bright"]
            elif attribute == "luxury":
                labels = ["expensive", "cheap"]
            elif attribute == "condition":
                labels = ["new", "used"]
            else:
                labels = ["yes "+attribute, "no "+attribute]

            results = clip(image_path, candidate_labels=labels)
            label = results[0]["label"]
            score = results[0]["score"]

            if label != labels[0]:
                score = 1-score

            dict[attribute] = score

    return dict



def add_clip_columns(df: pd.DataFrame, image_folder: pathlib.PosixPath, room_list: list, attribute_list: list, clip):
    """
    Use CLIP functions to add columns to data frame.
    Default images are computer generated images.
    Return DataFrame with additional columns.

    Input: DataFrame with column "image_name"
    Output: DataFrame with additional columns "default_image", "room_type", "scoring_dict"
    """
    print("start", df.head(), len(df))

    df["image_path"] = df["image_name"].apply(lambda x: \
        str(image_folder / str(x).split("_")[0] / x))

    # add column default_image
    df["default_image"] = df["image_path"].apply(lambda x: \
        identify_default_images(x, clip))

    # remove illustations/logos/adverts, but keep floor plans and images
    df = df[df["default_image"] != 2]\
        .reset_index().drop(columns="index")

    print("remove default", df.head(), len(df))

    # remove listings if <5 pictures
    source_id_count = pd.DataFrame(df['source_id'].value_counts()).reset_index()
    source_ids = source_id_count[source_id_count["count"]>=5]["source_id"]
    df = df[df['source_id'].isin(source_ids)]\
        .reset_index().drop(columns="index")

    print("remove if <5",df.head(), len(df))
    print("added default column")

    # add column room_type
    df["room_type"] = df["image_path"].apply(lambda x: \
        assign_room_type(x, room_list, clip))

    # remove unnecessary images, e.g. exteriors, stores, floor plans
    df = df[df["room_type"].isin(["kitchen", "bathroom", "toilet", "living room", "bedroom", "floor plan"])]\
        .reset_index().drop(columns="index")

    print("added room type",df.head(), len(df))

    # remove listings if <5 pictures
    source_id_count = pd.DataFrame(df['source_id'].value_counts()).reset_index()
    source_ids = source_id_count[source_id_count["count"]>=5]["source_id"]
    df = df[df['source_id'].isin(source_ids)]\
        .reset_index().drop(columns="index")

    print("remove if <5",df.head(), len(df))
    print("added room type column")

    # add column scoring_dict
    df["scoring_dict"] = df.apply(lambda x: \
        get_score(x["image_path"], x["room_type"], attribute_list, clip), \
            axis=1)

    print("added scoring dict column")

    return df



def average_scoring(df, attribute_list):
    """
    Sometimes, several pictures of the same room type are given.
    Compute average score for each room type in a listing.
    Return a DataFrame with 1 row per listing and columns for each attribute and room_type.

    Input: DataFrame 1 row per image
    Output: DataFrame 1 row per listing
    """

    mean_score = df.groupby(['source_id', 'room_type'])[attribute_list[0]].mean().reset_index()
    if len(attribute_list) > 1:
        for attribute in attribute_list[1:]:
            group_average = df.groupby(['source_id', 'room_type'])[attribute].mean().reset_index()[attribute]
            mean_score = mean_score.join(group_average)

    # pivot wider
    df_wide = mean_score.pivot(index="source_id", columns="room_type")

    # flatten MultiIndex columns -> luxury_bedroom, brightness_kitchen, etc.
    df_wide.columns = [
        f"{metric}_{room}".replace(" ", "_")
        for metric, room in df_wide.columns
    ]

    df_wide = df_wide.drop(columns=[attribute + "_floor_plan" for attribute in attribute_list])

    return df_wide.reset_index()
