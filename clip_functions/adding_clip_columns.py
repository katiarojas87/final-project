from clip_functions.clip_functions import assign_room_type, identify_default_images, get_score

import pathlib
import pandas as pd

import torch
from transformers import pipeline


clip = pipeline(
    task = "zero-shot-image-classification",
    model = "openai/clip-vit-base-patch32",
    dtype = torch.bfloat16,
    device=0
)


# Get the path of the current folder
data_path = pathlib.Path.cwd().parent() / "raw_data"

# import images.csv
image_df = pd.read_csv(data_path / "images.csv")

# define room list and attribute dict
RoomList = ["kitchen", "bathroom", "living room", "bedroom", "storage", "exterior", "entry", "shop", "floor plan", "control panel", "something else"]
AttributeList = ["luxury", "brightness", "modernity"]

def add_clip_columns(df: pd.DataFrame, image_folder: pathlib.PosixPath = data_path / "suumo_images", room_list: list = RoomList, attribute_list: list = AttributeList):
    """
    Use CLIP functions to add columns to data frame.
    Default images are computer generated images.
    Return DataFrame with additional columns.

    Input: DataFrame with column "image_path"
    Output: DataFrame with additional columns "default_image", "room_type", "scoring_dict"
    """

    df["image_path"] = df["image_name"].apply(lambda x: \
        identify_default_images(str(image_folder / str(x).split("_")[0] / x)))

    df["default_image"] = df["image_path"].apply(lambda x: \
        identify_default_images(x))

    df["room_type"] = df["image_path"].apply(lambda x: \
        assign_room_type(x, RoomList))

    df["scoring_dict"] = df.apply(lambda x: \
        get_score(x["image_path"], str(x["room_type"][0]), AttributeList), \
            axis=1)

    return df
