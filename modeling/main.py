import sys
import os
import warnings
import pickle
import pandas as pd
from utils.model_pipeline import get_pipeline
from utils.train_predict_model import train_model, predict_model
warnings.filterwarnings('ignore')


# Read Data
root_path = os.path.abspath(os.path.join(os.getcwd(), '..'))
sys.path.append(root_path)

file_path = os.path.abspath(
    os.path.join(os.getcwd(), '../mlops_fire_fighter/data'))

train = pd.read_pickle(file_path + '/train.pkl')
test = pd.read_pickle(file_path + '/test.pkl')

# Feature sets
# The lists below contain the features available in the data.
# target_feature: the target feature
# drop_features: features that will be dropped and are not considered
# cat_features: the categorical features
# num_features: the numerical features
# id_feature: the id feature
target = 'FirstPumpArriving_AttendanceTime_min'

drop_features = ['DateOfCall', 'CalYear', 'TimeOfCall', 'SpecialServiceType',
                 'Postcode_full', 'Postcode_district', 'UPRN', 'USRN',
                 'IncGeo_BoroughName', 'IncGeo_WardCode', 'IncGeo_WardNameNew',
                 'Easting_m', 'Northing_m', 'Easting_rounded',
                 'Northing_rounded', 'Latitude', 'Longitude', 'FRS',
                 'IncidentStationGround', 'FirstPumpArriving_AttendanceTime',
                 'SecondPumpArriving_AttendanceTime',
                 'SecondPumpArriving_DeployedFromStation', 'PumpCount',
                 'PumpHoursRoundUp', 'FirstPumpArriving_DeployedFromStation',
                 'IncGeo_BoroughCode']

cat_features = ['IncidentGroup', 'StopCodeDescription', 'PropertyCategory',
                'PropertyType', 'AddressQualifier', 'ProperCase',
                'IncGeo_WardName', 'PartOfDay']

num_features = ['HourOfCall', 'NumStationsWithPumpsAttending',
                'NumPumpsAttending', 'NationalCost', 'NumCalls',
                'DateOfCall_Month']

id_feature = 'IncidentNumber'

full_list_features = set(cat_features + num_features)

# Drop features & define modeling data sets
train = train.drop(columns=drop_features, axis=1)
test = test.drop(columns=drop_features, axis=1)

X_train = train.drop(columns=[target]+[id_feature])
y_train = train[target]
X_test = test.drop(columns=[target]+[id_feature])
y_test = test[target]

assert full_list_features == set(X_train.columns), 'missmatch in sets'
assert full_list_features == set(X_test.columns), 'missmatch in sets'

# Define algorithm options
algorithms = ['lgbm', 'xgboost', 'rf']
best_rmse = float('inf')
best_algorithm = None
best_predictions = None

# Try each algorithm and choose the one with the lowest RMSE
for algorithm in algorithms:
    pipe = get_pipeline(
        num_features=num_features,
        cat_features=cat_features,
        algorithm=algorithm,
        use_grid_search=False
    )

    model = train_model(
        pipeline=pipe,
        xtrain=X_train,
        ytrain=y_train
    )

    evaluation = predict_model(
        trained_model=model,
        xtest=X_test,
        ytest=y_test
    )

    rmse = evaluation['Root Mean Squared Error']
    if rmse < best_rmse:
        best_rmse = rmse
        best_algorithm = algorithm
        best_predictions = evaluation


# Use the best algorithm for final predictions
print(f"Best algorithm: {best_algorithm}")
print("Final Predictions:")
print(best_predictions)

# Save the best model
best_model = train_model(
    pipeline=get_pipeline(
        num_features=num_features,
        cat_features=cat_features,
        algorithm=best_algorithm,
        use_grid_search=False
    ),
    xtrain=X_train,
    ytrain=y_train
)

pickle_out = open('../mlops_fire_fighter/api/best_model.pkl', 'wb')
pickle.dump(best_model, pickle_out)
pickle_out.close()
