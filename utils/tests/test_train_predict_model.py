import pytest
from sklearn.pipeline import Pipeline
from sklearn.datasets import make_regression
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from utils.train_predict_model import train_model, predict_model


@pytest.fixture
def sample_pipeline():
    pipeline = Pipeline([
        ('regressor', LinearRegression())
    ])
    return pipeline


@pytest.fixture
def sample_dataset():
    X, y = make_regression(n_samples=100, n_features=2, random_state=42)
    X_train, X_test, y_train, y_test = train_test_split(X, y,
                                                        test_size=0.2,
                                                        random_state=42)
    return X_train, X_test, y_train, y_test


def test_train_model(sample_pipeline, sample_dataset):
    X_train, _, y_train, _ = sample_dataset
    model = train_model(sample_pipeline, X_train, y_train)
    assert model is not None


def test_predict_model(sample_pipeline, sample_dataset):
    _, X_test, _, y_test = sample_dataset
    model = train_model(sample_pipeline, X_test, y_test)
    evaluation = predict_model(model, X_test, y_test)

    assert evaluation == evaluation
