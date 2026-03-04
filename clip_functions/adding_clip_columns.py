from clip_functions.clip_functions import initialize_clip, assign_room_type, identify_default_images, get_score

import pathlib
import pandas as pd


def add_clip_columns(df: pd.DataFrame, image_folder: pathlib.PosixPath, room_list: list, attribute_list: list, clip):
    """
    Use CLIP functions to add columns to data frame.
    Default images are computer generated images.
    Return DataFrame with additional columns.

    Input: DataFrame with column "image_path"
    Output: DataFrame with additional columns "default_image", "room_type", "scoring_dict"
    """

    df["image_path"] = df["image_name"].apply(lambda x: \
        str(image_folder / str(x).split("_")[0] / x))

    # add column default_image
    df["default_image"] = df["image_path"].apply(lambda x: \
        identify_default_images(x, clip))

    # remove illustations/logos/adverts, but keep floor plans and images
    df = df[df["default_image"] != 2]\
        .reset_index().drop(columns="index")

    print("added default column")

    # add column room_type
    df["room_type"] = df["image_path"].apply(lambda x: \
        assign_room_type(x, room_list, clip))

    # remove unnecessary images, e.g. exteriors, stores, floor plans
    df = df[df["room_type"].isin(["kitchen", "bathroom", "toilet", "living room", "bedroom", "floor plan"])]\
        .reset_index().drop(columns="index")

    print("added room type column")

    # add column scoring_dict
    df["scoring_dict"] = df.apply(lambda x: \
        get_score(x["image_path"], str(x["room_type"][0]), attribute_list, clip), \
            axis=1)

    print("added scoring dict column")

    return df
