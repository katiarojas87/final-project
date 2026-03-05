#core
import pandas as pd
import pathlib

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



def preprocess_features(X):
    """
    This function creates a preprocessor pipeline and returns X_processed.
    """
    def create_sklearn_preprocessor() -> ColumnTransformer:
        num_features = ["price_man_yen","area_sqm","year_built","floor_number","floors_total","walk_minutes"]

        num_transformer = Pipeline ([
            # ("imputer", SimpleImputer(strategy="mean")), #Missing values, normally distributed
            # ("standard_scaler", StandardScaler()), #Features on different scales, linear models
            # ("minmax_scaler", MinMaxScaler()), #When you need values between 0–1
            ("robust_scaler", RobustScaler()) #Data with lots of outliers
        ])

        cat_features = ["layout","address","nearest_station"]

        cat_transformer = Pipeline ([
            ("ohe", OneHotEncoder(drop = "if_binary",handle_unknown="ignore")), #Nominal data (no order) e.g. region, room_type
            ("ordinal_encoder", OrdinalEncoder()), #Ordered categories e.g. small < medium < large
            ("imputer", SimpleImputer(strategy="most_frequent")) #Missing categorical values
        ])


        final_preprocessor = ColumnTransformer([
            ('num_transformer', num_transformer, num_features),
            ('cat_transformer', cat_transformer, cat_features)
        ])

        return final_preprocessor


    print("\nPreprocessing features...")

    preprocessor = create_sklearn_preprocessor()
    X_processed = preprocessor.fit_transform(X)

    print("✅ X_processed, with shape", X_processed.shape)

    return X_processed
