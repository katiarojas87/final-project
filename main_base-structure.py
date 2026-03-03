import torch
from transformers import pipeline
import pandas as pd
import pathlib

from clip_functions.clip_functions import assign_room_type, identify_default_images, get_score
from clip_functions.adding_clip_columns import add_clip_columns

clip = pipeline(
    task = "zero-shot-image-classification",
    model = "openai/clip-vit-base-patch32",
    dtype = torch.bfloat16,
    device=0
)

# Get the path of the current folder
data_path = pathlib.Path.cwd() / "raw_data"

# import images.csv
image_df = pd.read_csv(data_path / "images.csv")

# define room list and attribute dict
RoomList = ["kitchen", "bathroom", "living room", "bedroom", "storage", "exterior", "entry", "shop", "floor plan", "control panel", "something else"]
AttributeList = ["luxury", "brightness", "modernity"]

add_clip_columns(df = image_df, image_folder = data_path / "suumo_images", room_list = RoomList, attribute_list = AttributeList)
