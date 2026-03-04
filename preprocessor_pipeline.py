#core
!pip install scikit-learn --break-system-packages -q
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

# Model selection
from sklearn.model_selection import train_test_split, cross_val_score

#load dataset(double check name of the main dataframe)
data_path = pathlib.Path(".")
data = pd.read_csv(data_path / "raw_data/listings_with_scores.csv")
data.head()

#define features X and target
X = data.drop(columns=["price_man_yen"]).copy()
y = data["price_man_yen"]

# train test split
#X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20)

#(f"X_train\t{X_train.shape}", f"X_test\t{X_test.shape}", f"y_train\t{y_train.shape}", f"y_test\t{y_test.shape}")

num_transformer = Pipeline ([
    ("imputer", SimpleImputer(strategy="mean")), #Missing values, normally distributed
    ("standard_scaler", StandardScaler()), #Features on different scales, linear models
    ("minmax_scaler", MinMaxScaler()), #When you need values between 0–1
    ("robust_scaler", RobustScaler()) #Data with lots of outliers
])
cat_transformer = Pipeline ([
    ("ohe", OneHotEncoder(drop = "if_binary",handle_unknown="ignore")), #Nominal data (no order) e.g. region, room_type
    ("ordinal_encoder", OrdinalEncoder()), #Ordered categories e.g. small < medium < large
    ("imputer", SimpleImputer(strategy="most_frequent")) #Missing categorical values
])

# Preprocessing "num_transformer" and "cat_transfomer"
preprocessor = ColumnTransformer([
    ('num_transformer', num_transformer, ['column1', 'column2']),
    ('cat_transformer', cat_transformer, ['column1', 'column2'])
])
