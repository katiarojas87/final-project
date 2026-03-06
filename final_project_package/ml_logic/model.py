import numpy as np
import time

# Timing the TF import
start = time.perf_counter()

from sklearn.linear_model import LinearRegression
from sklearn.neighbors import KNeighborsRegressor
from sklearn.model_selection import cross_validate
from sklearn.metrics import mean_squared_error

end = time.perf_counter()
print(f"\n✅ TensorFlow loaded ({round(end - start, 2)}s)")



def initialize_model():
    """
    Initialize the model
    """
    model = LinearRegression()
    #model = KNeighborsRegressor()

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
    cv = cross_validate(model, X_train, y_train, cv = 5)["test_score"].mean()

    model = model.fit(
        X_train,
        y_train
    )

    print(f"✅ Model trained on {len(X_train)} rows with cross validation R2-score: {round(cv, 2)}")

    return model, cv


def evaluate_model(
        model,
        X_test: np.ndarray,
        y_test: np.ndarray
    ):
    """
    Evaluate trained model performance on the dataset
    """

    if model is None:
        print(f"\n❌ No model to evaluate")
        return None

    pred = model.predict(
        x=X_test
    )

    mse = mean_squared_error(y_test, pred)

    print(f"✅ Model evaluated, MSE: {round(mse, 2)}")

    return mse
