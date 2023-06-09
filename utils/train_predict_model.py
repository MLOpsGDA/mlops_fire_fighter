import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV
from typing import Any, Union
from sklearn.metrics import (mean_squared_error,
                             mean_absolute_error)


def train_model(
    pipeline: Pipeline,
    xtrain: pd.DataFrame,
    ytrain: pd.Series
) -> Any:
    """Function to train the model
    Args:
        pipeline (Pipeline): sklearn pipeline
        xtrain (pd.DataFrame): train dataframe
        ytrain (pd.Series): vector of target feature
    Returns:
        Any: the trained model
    """
    model = pipeline.fit(xtrain, ytrain)

    return model


def predict_model(
    trained_model: Union[Pipeline, GridSearchCV],
    xtest: pd.DataFrame,
    ytest: pd.Series,
) -> Any:
    """Function to predict the class of the label
    Args:
        trained_model (Union[Pipeline, GridSearchCV]): trained model pipeline
            or GridSearchCV object
        xtest (pd.DataFrame): dataframe to be predicted on
        ytest (pd.Series): vector of target feature
    Returns:
        Any: calculated evaluation values
    """
    if isinstance(trained_model, GridSearchCV):
        trained_model = trained_model.best_estimator_

    y_pred = trained_model.predict(xtest)

    mse = mean_squared_error(ytest, y_pred)
    mae = mean_absolute_error(ytest, y_pred)
    rmse = mean_squared_error(ytest, y_pred, squared=False)

    print("Mean Squared Error:", mse)
    print("Mean Absolute Error:", mae)
    print("Root Mean Squared Error:", rmse)
