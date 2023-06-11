from lightgbm import LGBMRegressor
from sklearn.impute import SimpleImputer
from tubular.nominal import GroupRareLevelsTransformer
from tubular.nominal import NominalToIntegerTransformer
from tubular.capping import CappingTransformer
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from typing import List, Dict, Union
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from sklearn.model_selection import GridSearchCV


def get_pipeline(
    num_features: List[str],
    cat_features: List[str],
    algorithm: str,
    tuning_params: Dict[str, List[Union[int, float, str]]] = {},
    use_grid_search: bool = True,
    custom_hyperparameters: Dict[str, Union[int, float, str]] = None
) -> Pipeline:
    """This function returns the model's pipeline
    Args:
        num_features (List of str): list of numerical features
        cat_features (List of str): list of categorical features
        algorithm (str): algorithm can be chosen from the follwing options,
        lgbm, xgboost, and rf
        tuning_params (Dict[str, List[Union[int, float, str]]]): dictionary
        including a range of HP values specified by user. Defaulto to empty.
        use_grid_search (bool, optional): if grid search should be applied or
        not. Defaults to True.
        custom_hyperparameters (Dict[str, Union[int, float, str]], optional):
        dictionary of specific HP values specified by user. Defaults to None.
    Raises:
        ValueError: if the chosen algorithm not lgbm, xgboost, or rf,
        pipeline will fail
    Returns:
        Pipeline: sklearn pipeline
    """

    # define the grouping and capping transformers
    StopCodeDescription_transformer = GroupRareLevelsTransformer(
        columns='StopCodeDescription',
        cut_off_percent=0.05,
        verbose=False
    )
    PropertyCategory_transformer = GroupRareLevelsTransformer(
        columns='PropertyCategory',
        cut_off_percent=0.05,
        verbose=False
    )
    PropertyType_transformer = GroupRareLevelsTransformer(
        columns='PropertyType',
        cut_off_percent=0.03,
        verbose=False
    )
    AddressQualifier_transformer = GroupRareLevelsTransformer(
        columns='AddressQualifier',
        cut_off_percent=0.05,
        verbose=False
    )
    IncGeo_WardName_transformer = GroupRareLevelsTransformer(
        columns='IncGeo_WardName',
        cut_off_percent=0.003,
        verbose=False
    )
    cap_national_cost = CappingTransformer(
        capping_values={'NationalCost': [0, 400]}
    )
    PartOfDay_transformer = NominalToIntegerTransformer(
        columns=cat_features,
        copy=True
    )

    preprocessor = Pipeline([('StopCodeDescription_transformer',
                              StopCodeDescription_transformer),
                             ('PropertyCategory_transformer',
                              PropertyCategory_transformer),
                             ('PropertyType_transformer',
                              PropertyType_transformer),
                             ('AddressQualifier_transformer',
                              AddressQualifier_transformer),
                             ('IncGeo_WardName_transformer',
                              IncGeo_WardName_transformer),
                             ('cap_national_cost',
                              cap_national_cost),
                             ('PartOfDay_transformer',
                              PartOfDay_transformer)
                             ],
                            verbose=False)

    if algorithm == 'lgbm':
        estimator = LGBMRegressor(
            random_state=24,
            objective='regression_l1',
            n_jobs=-1
        )
        param_grid = tuning_params
    elif algorithm == 'xgboost':
        estimator = XGBRegressor(
            random_state=24,
            objective='reg:squaredlogerror',
            n_jobs=-1
        )
        param_grid = tuning_params
    elif algorithm == 'rf':
        estimator = RandomForestRegressor(
            random_state=24,
            n_jobs=-1
        )
        param_grid = tuning_params
    else:
        raise ValueError(f"Invalid algorithm {algorithm}, "
                         "Choose from 'lgbm', 'xgboost', or 'rf'. ")

    num_features_transformer = Pipeline(
        steps=[('imputer', SimpleImputer(strategy='median'))])

    cat_features_transformer = Pipeline(
        steps=[
            ('imputer', SimpleImputer(strategy='most_frequent'))])

    col_transformers = ColumnTransformer(
        transformers=[
            ('numerical', num_features_transformer, num_features),
            ('categorical', cat_features_transformer, cat_features),
        ],
        remainder='passthrough')

    pipe = Pipeline(
        steps=[('preprocessor', preprocessor),
               ('col_transformers', col_transformers),
               ('estimator', estimator)
               ],
        verbose=False)

    if use_grid_search:
        grid_search = GridSearchCV(
            pipe,
            param_grid=param_grid,
            cv=5,
            scoring='neg_mean_squared_error',
            n_jobs=-1
        )

        return grid_search

    elif custom_hyperparameters:
        pipe.set_params(**custom_hyperparameters)

    return pipe
