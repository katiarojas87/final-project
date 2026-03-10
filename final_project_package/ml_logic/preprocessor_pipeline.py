# core
import pandas as pd
import numpy as np

# pipeline
from sklearn.discriminant_analysis import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer

# Imputers
from sklearn.impute import SimpleImputer

# Numerical scalers
from sklearn.preprocessing import RobustScaler

# Categorical encoders
from sklearn.preprocessing import OrdinalEncoder, TargetEncoder, FunctionTransformer


def aggregate_columns(X):
    X = np.asarray(X)
    return np.nanmean(X, axis=1).reshape(-1, 1)


def mean_luxury_name(transformer, input_features):
    return ["mean_luxury"]


def mean_brightness_name(transformer, input_features):
    return ["mean_brightness"]


def mean_condition_name(transformer, input_features):
    return ["mean_condition"]


mean_luxury_transformer = FunctionTransformer(
    aggregate_columns,
    feature_names_out=mean_luxury_name
)

mean_brightness_transformer = FunctionTransformer(
    aggregate_columns,
    feature_names_out=mean_brightness_name
)

mean_condition_transformer = FunctionTransformer(
    aggregate_columns,
    feature_names_out=mean_condition_name
)


def get_fitted_preprocessor(X_train, y_train):
    """
    Create and fit a preprocessing pipeline.
    """

    num_features = ["area_sqm", "year_built", "floor_number", "floors_total", "walk_minutes"]

    num_transformer = Pipeline([
        ("robust_scaler", RobustScaler())
    ])

    base_layout_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("ordinal", OrdinalEncoder(
            categories=[["R", "K", "DK", "LDK"]],
            handle_unknown="use_encoded_value",
            unknown_value=-1
        ))
    ])

    building_period_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("ordinal", OrdinalEncoder(
            categories=[["pre 1981", "1981 to 2000", "post 2000"]],
            handle_unknown="use_encoded_value",
            unknown_value=-1
        ))
    ])

    station_pipe = Pipeline([
        ("target_encoder", TargetEncoder(target_type="continuous")),
        ("scaler", RobustScaler())
    ])

    final_preprocessor = ColumnTransformer([
        ("keep_rooms", "passthrough", ["rooms_num"]),
        ("station_transformer", station_pipe, ["nearest_station"]),
        ("num_transformer", num_transformer, num_features),
        ("ordinal_transformer", base_layout_pipe, ["base_layout"]),
        ('building_period_transformer', building_period_pipe, ['building_period']),
        ("mean_luxury_transformer", mean_luxury_transformer,
         ["luxury_bathroom", "luxury_bedroom", "luxury_kitchen", "luxury_living_room", "luxury_toilet"]),
        ("mean_brightness_transformer", mean_brightness_transformer,
         ["brightness_bathroom", "brightness_bedroom", "brightness_kitchen", "brightness_living_room", "brightness_toilet"]),
        ("mean_condition_transformer", mean_condition_transformer,
         ["condition_bathroom", "condition_bedroom", "condition_kitchen", "condition_living_room", "condition_toilet"])
    ], remainder="drop")

    print("\nPreprocessing features...")
    preprocessor = final_preprocessor.fit(X_train, y_train)
    print("✅ returned preprocessor")

    return preprocessor
