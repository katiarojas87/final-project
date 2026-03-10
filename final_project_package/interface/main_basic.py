import pandas as pd
import numpy as np
import pathlib
import os
import pickle
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split

from final_project_package.ml_logic.data_clean import initialize_clip, data_clean, add_clip_columns, average_scoring
from final_project_package.ml_logic.model import initialize_model, train_model, evaluate_model
from final_project_package.ml_logic.preprocessor_pipeline import get_fitted_preprocessor
from scipy.stats import zscore

def load_data(path_to_project: str, nr_batches):

    # Get the path of the current folder
    data_path = pathlib.Path(path_to_project)

    # import images.csv
    image_full = pd.read_csv(data_path / "raw_data/images.csv")

    # import listings.csv
    listings_full = pd.read_csv(data_path / "raw_data/listings.csv")

    # run data cleaning function
    listings_full, image_full = data_clean(listings_full, image_full)

    # define room list and attribute dict
    RoomList = ["kitchen", "bathroom", "toilet", "living room", "bedroom", "walk-in closet", "closet", "entry inside", "exterior", "shop", "floor plan", "control panel", "entry outside", "balcony"]
    AttributeList = ["luxury", "brightness", "condition"]

    # initialize clip
    clip = initialize_clip()

    previous_listing = 0
    for listing in np.linspace(len(listings_full)/nr_batches,len(listings_full), nr_batches).astype("int"):
        start = previous_listing
        stop = listing

        previous_listing = listing

        # df into batch
        listings_df = listings_full[start:stop]\
            .reset_index().drop(columns="index")
        source_id = listings_df["source_id"]
        image_df = image_full[image_full["source_id"].isin(source_id)]\
            .reset_index().drop(columns="index")

        print("files imported, listings cleaned, clip initialized")

        # add columns and remove unwanted images
        image_df = add_clip_columns(df = image_df,
                                    image_folder = data_path / "raw_data/suumo_images",
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
        os.makedirs("data_dump", exist_ok=True)
        file_exists = os.path.isfile(data_path / "data_dump/images_cleaned.csv")
        if file_exists:
            image_df.to_csv(data_path / "data_dump/images_cleaned.csv", mode = "a", header=False, index=False)
        else:
            image_df.to_csv(data_path / "data_dump/images_cleaned.csv", index = False)

        file_exists = os.path.isfile(data_path / "data_dump/listings_cleaned.csv")
        if file_exists:
            listings_df.to_csv(data_path / "data_dump/listings_cleaned.csv", mode = "a", header=False, index=False)
        else:
            listings_df.to_csv(data_path / "data_dump/listings_cleaned.csv", index = False)

        # scoring dict into column for each attribute
        details_df = pd.json_normalize(image_df['scoring_dict'])
        image_df = image_df.join(details_df)

        # compute average score per room type and listing
        average_scores = average_scoring(image_df, AttributeList)
        print("average scores computed")

        # merge listings to include average scores per room type
        listings_df = listings_df.merge(average_scores, on = "source_id")

        # write final data to csv
        file_exists = os.path.isfile(data_path / "data_dump/listings_with_scores.csv")
        if file_exists:
            listings_df.to_csv(data_path / "data_dump/listings_with_scores.csv", mode = "a", header=False, index=False)
        else:
            listings_df.to_csv(data_path / "data_dump/listings_with_scores.csv", index = False)
        print("listings_with_scores.csv saved")

    return listings_df



def preprocess(
    path_to_project: str,
    split_ratio: float = 0.3, # 0.3 default test_size
    ):

    data_path = pathlib.Path(path_to_project)
    data = pd.read_csv(data_path / "data_dump/listings_with_scores.csv")

    def fix_floating(row):
        if row['floor_number'] > row['floors_total']:
            row['floors_total'] = row['floor_number'] * 2

        return row

    data = data.apply(fix_floating, axis=1)

    data['price_zscore'] = zscore(data['price_man_yen'])
    data = data[data['price_zscore'].abs() <= 3]
    data = data.drop('price_zscore', axis=1)

    data["building_period"] = pd.cut(
        data["year_built"],
        bins=[0, 1980, 2000, float("inf")],
        labels=["pre 1981", "1981 to 2000", "post 2000"]
    )

    X = data.drop(columns=["price_man_yen"]).copy()
    y = data["price_man_yen"]

    def preprocess_y(y):
        return np.log1p(y)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=split_ratio)
    preprocesser = get_fitted_preprocessor(X_train, y_train)

    # Save the preprocessor to a file
    filename = data_path / "data_dump/preprocessor.pkl"
    with open(filename, 'wb') as file:
        pickle.dump(preprocesser, file)

    X_train_preprocessed = pd.DataFrame(preprocesser.transform(X_train), columns=preprocesser.get_feature_names_out(), index=X_train.index)
    X_test_preprocessed = pd.DataFrame(preprocesser.transform(X_test), columns=preprocesser.get_feature_names_out(), index=X_test.index)
    y_train_preprocessed = pd.Series(preprocess_y(y_train))
    y_test_preprocessed = pd.Series(preprocess_y(y_test))

    X_train_preprocessed.to_csv(data_path / "data_dump/X_train_preprocessed.csv", index = False)
    X_test_preprocessed.to_csv(data_path / "data_dump/X_test_preprocessed.csv", index = False)
    y_train_preprocessed.to_csv(data_path / "data_dump/y_train_preprocessed.csv", index = False)
    y_test_preprocessed.to_csv(data_path / "data_dump/y_test_preprocessed.csv", index = False)

    return X_train_preprocessed, X_test_preprocessed, y_train_preprocessed, y_test_preprocessed



def train(
        path_to_project: str
    ):

    """
    - Load processed training data from csv table
    - Train on the preprocessed dataset
    - Store model and cv-score

    Return mse as a float
    """

    # Load processed data
    data_path = pathlib.Path(path_to_project)
    X_train = pd.read_csv(data_path / "data_dump/X_train_preprocessed.csv")
    y_train = pd.read_csv(data_path / "data_dump/y_train_preprocessed.csv")

    # depending on how Lances saves the preprocessed data: create (X_train_processed, y_train)

    # initialize model
    model = initialize_model()

    # train model
    model, cv = train_model(
            model,
            X_train,
            y_train
            )

    # Save the model to a file
    filename = data_path / "data_dump/finalized_model.sav"
    with open(filename, 'wb') as file:
        pickle.dump(model, file)

    print("✅ train() done \n")

    return model, cv


def evaluate(
        path_to_project: str
    ):

    data_path = pathlib.Path(path_to_project)
    X_test = pd.read_csv(data_path / "data_dump/X_test_preprocessed.csv")
    y_test = pd.read_csv(data_path / "data_dump/y_test_preprocessed.csv")

    filename = data_path / "data_dump/finalized_model.sav"

    # Load the model
    with open(filename, 'rb') as file:
        model = pickle.load(file)

    mse = evaluate_model(
        model,
        X_test,
        y_test
        )

    return mse


def pred(
        path_to_project: str,
        X_new: pd.DataFrame = None
    ) -> np.ndarray:
    """
    Make a prediction using the latest trained model
    """

    print("\n⭐️ Use case: predict")

    # Load the model
    data_path = pathlib.Path(path_to_project)
    filename = data_path / "data_dump/finalized_model.sav"

    with open(filename, 'rb') as file:
        model = pickle.load(file)
    assert model is not None

    # Load the preprocessor
    filename = data_path / "data_dump/preprocessor.sav"
    with open(filename, 'rb') as file:
        preprocessor = pickle.load(file)

    X_processed = preprocessor.transform(X_new)
    y_pred = model.predict(X_processed)

    print("\n✅ prediction done: ", y_pred, y_pred.shape, "\n")
    return y_pred


if __name__ == '__main__':
    load_data(".", 50)
#    preprocess(".", 0.3)
#    train(".")
#    evaluate(".")
#    pred()
