from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from lightgbm import LGBMRegressor
from xgboost import XGBRegressor

from utils.model_pipeline import get_pipeline


def test_get_pipeline_lgbm():
    num_features = ['num_feature1', 'num_feature2']
    cat_features = ['cat_feature1', 'cat_feature2']
    algorithm = 'lgbm'
    tuning_params = {'param1': [1, 2, 3]}
    use_grid_search = False

    pipeline = get_pipeline(num_features,
                            cat_features,
                            algorithm,
                            tuning_params,
                            use_grid_search)

    assert isinstance(pipeline, (Pipeline, LGBMRegressor))


def test_get_pipeline_xgboost():
    num_features = ['num_feature1', 'num_feature2']
    cat_features = ['cat_feature1', 'cat_feature2']
    algorithm = 'xgboost'
    tuning_params = {'param1': [1, 2, 3]}
    use_grid_search = False

    pipeline = get_pipeline(num_features,
                            cat_features,
                            algorithm,
                            tuning_params,
                            use_grid_search)

    assert isinstance(pipeline, (Pipeline, XGBRegressor))


def test_get_pipeline_rf():
    num_features = ['num_feature1', 'num_feature2']
    cat_features = ['cat_feature1', 'cat_feature2']
    algorithm = 'rf'
    tuning_params = {'param1': [1, 2, 3]}
    use_grid_search = False

    pipeline = get_pipeline(num_features,
                            cat_features,
                            algorithm,
                            tuning_params,
                            use_grid_search)

    assert isinstance(pipeline, (Pipeline, RandomForestRegressor))
