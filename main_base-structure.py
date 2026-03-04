import pandas as pd
import pathlib
from preprocess import data_clean
from clip_functions.adding_clip_columns import add_clip_columns
from clip_functions.clip_functions import initialize_clip

def load_data(path_to_project: str):
    # Get the path of the current folder
    data_path = pathlib.Path(path_to_project) / "raw_data"

    # import images.csv
    image_df = pd.read_csv(data_path / "images.csv")
    image_df = image_df[0:30]

    # import listings.csv (TO BE FINISHED)
    listings_df = pd.read_csv(data_path / "listings.csv")
    listings_df = listings_df[0:30]

    # run lance data cleaning functions (TO BE FINISHED)
    listings_df, image_df = data_clean(listings_df, image_df)


    # define room list and attribute dict
    RoomList = ["kitchen", "bathroom", "toilet", "living room", "bedroom", "walk-in closet", "closet", "entry inside", "exterior", "shop", "floor plan", "control panel", "entry outside"]
    AttributeList = ["luxury", "brightness", "modernity"]

    # initialize clip
    clip = initialize_clip()

    # add columns and remove unwanted images
    image_df = add_clip_columns(df = image_df,
                                image_folder = data_path / "suumo_images",
                                room_list = RoomList,
                                attribute_list = AttributeList,
                                clip = clip)

    # save csv
    image_df.to_csv("images_cleaned.csv")
    listings_df.to_csv("listings_cleaned.csv")

    # merge listings to include scores and room type (TO BE FINISHED)
    #intermediary step
    listings_df.merge(image_df)

    return listings_df

if __name__ == '__main__':
    load_data(".")
#    preprocess(min_date='2009-01-01', max_date='2015-01-01')
#    train(min_date='2009-01-01', max_date='2015-01-01')
#    evaluate(min_date='2009-01-01', max_date='2015-01-01')
#    pred()
