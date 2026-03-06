#core
import pandas as pd
import re
import numpy as np

#pipeline
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer

# Imputers
from sklearn.impute import SimpleImputer

# Numerical scalers
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import RobustScaler
from sklearn.preprocessing import PowerTransformer

# Categorical encoders
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import OrdinalEncoder
from sklearn.preprocessing import TargetEncoder
from sklearn.preprocessing import FunctionTransformer


def get_fitted_preprocessor(X_train):
    """
    This function creates a preprocessor pipeline and returns X_processed.
    """
    def create_sklearn_preprocessor() -> ColumnTransformer:
        num_features = ["area_sqm","year_built","floor_number","floors_total","walk_minutes"]

        num_transformer = Pipeline ([
            # ("imputer", SimpleImputer(strategy="mean")), #Missing values, normally distributed
            # ("standard_scaler", StandardScaler()), #Features on different scales, linear models
            # ("minmax_scaler", MinMaxScaler()), #When you need values between 0–1
            ("robust_scaler", RobustScaler()) #Data with lots of outliers
        ])

        # cat_features = ["address"] this is unused yet.

        base_layout_pipe = Pipeline([
            'ordinal', OrdinalEncoder(categories=[["R", "K", "DK", "LDK"]])
        ])

        station_pipe = Pipeline([
            ("ohe", OneHotEncoder(
                min_frequency=15,        # ✅ tune this (10/20/50 depending on dataset size)
                sparse_output=False
            ))
        ])

        # This propressor drops the old index, the image count, the address, URL, 
        final_preprocessor = ColumnTransformer([
            ("keep_columns", "passthrough", ["source_id", "rooms_num"]),
            ('num_transformer', num_transformer, num_features),
            ('nearest_station_tranformer', station_pipe, ["nearest_station"]),
            ('ordinal', base_layout_pipe, ['base_layout'])
            ], remainder= "drop"
        )

        return final_preprocessor


    print("\nPreprocessing features...")

    preprocessor = create_sklearn_preprocessor().fit(X_train)

    print("✅ returned preprocessor")

    return preprocessor
