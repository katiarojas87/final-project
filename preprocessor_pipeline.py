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

layout_parser = FunctionTransformer(parse_layout, feature_names_out="one-to-one")


def preprocess_features(X):
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

        layout_pipe = Pipeline([
            ("encode", ColumnTransformer(
                transformers=[
                    # ordinal numeric feature (ordered)
                    ("rooms_num", Pipeline([
                        ("impute", SimpleImputer(strategy="median")),
                    ]), ["rooms_num"]),

                    # one-hot for the letter type
                    ("base_layout", OneHotEncoder(handle_unknown="ignore", sparse_output=False), ["base_layout"]),
                ],
                remainder="passthrough"
            ))
        ])

        # cat_features = ["address"] this is unused yet.

        station_pipe = Pipeline([
            ("ohe", OneHotEncoder(
                handle_unknown="ignore",
                min_frequency=15,        # ✅ tune this (10/20/50 depending on dataset size)
                sparse_output=False
            ))
        ])


        final_preprocessor = ColumnTransformer([
            ('num_transformer', num_transformer, num_features),
            ('layout_transformer', layout_pipe, ["layout"] ),
            ('nearest_station_tranformer', station_pipe, ["nearest_station"])
        ])

        return final_preprocessor


    print("\nPreprocessing features...")

    preprocessor = create_sklearn_preprocessor()
    X_processed = preprocessor.fit_transform(X)

    print("✅ X_processed, with shape", X_processed.shape)

    return X_processed
