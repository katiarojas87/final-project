import pandas as pd
import numpy as np
import pathlib
import os
from sklearn.compose import ColumnTransformer

from clip_functions.clip_functions import initialize_clip
from clip_functions.data_clean import data_clean
from clip_functions.adding_clip_columns import add_clip_columns, average_scoring

def load_data(path_to_project: str, nr_batches):

    # Get the path of the current folder
    data_path = pathlib.Path(path_to_project) / "raw_data"

    # import images.csv
    image_df = pd.read_csv(data_path / "images.csv")

    # import listings.csv
    listings_df = pd.read_csv(data_path / "listings.csv")

    previous_listing = 0
    for listing in np.linspace(len(listings_df)/nr_batches,len(listings_df), nr_batches):
        start = previous_listing
        stop = listing

        previous_listing = listing

        # TEMPORARY!!!
        listings_df = listings_df[start:stop]\
            .reset_index().drop(columns="index")
        source_id = listings_df["source_id"]
        image_df = image_df[image_df["source_id"].isin(source_id)]\
            .reset_index().drop(columns="index")

        # run data cleaning function
        listings_df, image_df = data_clean(listings_df, image_df)

        # define room list and attribute dict
        RoomList = ["kitchen", "bathroom", "toilet", "living room", "bedroom", "walk-in closet", "closet", "entry inside", "exterior", "shop", "floor plan", "control panel", "entry outside"]
        AttributeList = ["luxury", "brightness", "modernity"]

        # initialize clip
        clip = initialize_clip()

        print("files imported, listings cleaned, clip initialized")

        # add columns and remove unwanted images
        image_df = add_clip_columns(df = image_df,
                                    image_folder = data_path / "suumo_images",
                                    room_list = RoomList,
                                    attribute_list = AttributeList,
                                    clip = clip)

        # remove listings if <5 pictures
        source_id_count = pd.DataFrame(image_df['source_id'].value_counts()).reset_index()
        source_ids = source_id_count[source_id_count["count"]>=5]["source_id"]
        listings_df = listings_df[listings_df['source_id'].isin(source_ids)]\
            .reset_index().drop(columns="index")
        image_df = image_df[image_df['source_id'].isin(source_ids)]\
            .reset_index().drop(columns="index")

        # save csv
        file_exists = os.path.isfile("images_cleaned.csv")
        if file_exists:
            image_df.to_csv("images_cleaned.csv", mode = "a")
        else:
            image_df.to_csv("images_cleaned.csv")

        file_exists = os.path.isfile("listings_cleaned.csv")
        if file_exists:
            listings_df.to_csv("listings_cleaned.csv", mode = "a")
        else:
            listings_df.to_csv("listings_cleaned.csv")

        # scoring dict into column for each attribute
        details_df = pd.json_normalize(image_df['scoring_dict'])
        image_df = image_df.join(details_df)

        # compute average score per room type and listing
        average_scores = average_scoring(image_df, AttributeList)
        print("average scores computed")

        # merge listings to include average scores per room type
        listings_df.join(average_scores)

        # write final data to csv
        file_exists = os.path.isfile("listings_with_scores.csv")
        if file_exists:
            listings_df.to_csv("listings_with_scores.csv", mode = "a")
        else:
            listings_df.to_csv("listings_with_scores.csv")
        print("listings_with_scores.csv saved")

    return listings_df

if __name__ == '__main__':
    load_data(".", 50)
#    preprocess(min_date='2009-01-01', max_date='2015-01-01')
#    train(min_date='2009-01-01', max_date='2015-01-01')
#    evaluate(min_date='2009-01-01', max_date='2015-01-01')
#    pred()
