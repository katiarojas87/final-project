from clip_functions.clip_functions import initialize_clip, assign_room_type, identify_default_images, get_score

import pathlib
import pandas as pd


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

    df_wide.drop(columns=[attribute + "_floor_plan" for attribute in attribute_list])

    return df_wide.reset_index()
