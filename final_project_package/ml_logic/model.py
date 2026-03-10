import numpy as np
import time

# Timing the TF import
start = time.perf_counter()

from sklearn.linear_model import LinearRegression
from sklearn.neighbors import KNeighborsRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_validate
from sklearn.metrics import root_mean_squared_error, mean_squared_error

end = time.perf_counter()
print(f"\n✅ TensorFlow loaded ({round(end - start, 2)}s)")



def initialize_model():
    """
    Initialize the model
    """
    #model = LinearRegression()
    #model = KNeighborsRegressor()
    model = RandomForestRegressor(random_state=42, max_depth= None, n_estimators= 100)

    print("✅ Model initialized")

    return model


def train_model(
        model,
        X_train: np.ndarray,
        y_train: np.ndarray
    ):
    """
    Fit the model and return model and cross-validation
    """
    cv = cross_validate(model, X_train, y_train, cv = 5, scoring=['r2', 'neg_mean_squared_error'])

    model = model.fit(
        X_train,
        y_train
    )

    print(f"✅ Model trained on {len(X_train)} rows with cross validation R2-score:{round(cv['test_r2'].mean(), 6)} and the MSE is {round(cv['test_neg_mean_squared_error'].mean(), 6)}")

    return model, cv


def evaluate_model(
        model,
        X_test: np.ndarray,
        y_test_log: np.ndarray
    ):
    """
    Evaluate trained model performance on the dataset
    """

    if model is None:
        print(f"\n❌ No model to evaluate")
        return None

    y_pred_log = model.predict(
        X=X_test
    )

    y_test = np.expm1(y_test_log)
    y_pred = np.expm1(y_pred_log)

    rmse = root_mean_squared_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)

    print(f"✅ Model evaluated, MSE: {round(mse, 7)} and RMSE: {round(rmse, 7)}")

    return mse, rmse
