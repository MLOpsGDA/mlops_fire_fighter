from fastapi import FastAPI
from pydantic import BaseModel
import pickle
import pandas as pd

# Define the FastAPI app
app = FastAPI(title='MLOpsGDA API',
              description='MLOpsGDA API for the firefighter project',
              version='1.0.1',
              openapi_tags=[
                  {
                      'name': 'Home',
                      'description': 'API functionality'
                  },
                  {
                      'name': 'Predictions',
                      'description': 'Get predicted response time'
                  }
              ])

# Load the pipeline model
with open('../api/pipe.pkl', 'rb') as pickle_in:
    pipe = pickle.load(pickle_in)


# Define the request payload model using Pydantic
class PredictionPayload(BaseModel):
    HourOfCall: int
    IncidentGroup: str
    StopCodeDescription: str
    PropertyCategory: str
    PropertyType: str
    AddressQualifier: str
    ProperCase: str
    IncGeo_WardName: str
    NumStationsWithPumpsAttending: float
    NumPumpsAttending: float
    NumCalls: float
    DateOfCall_Month: int
    PartOfDay: str
    NationalCost: float

# Expose the prediction functionality


@app.get('/', tags=['Home'])
def index():
    return {'message': 'API is working properly!'}


# Define the prediction endpoint
@app.post('/predict', tags=['Predictions'])
def predict(payload: PredictionPayload):
    # Convert the payload to a DataFrame
    df = pd.DataFrame([payload.dict()])

    # Make predictions using the pipeline model
    predictions = pipe.predict(df)

    # Return the predictions as a response
    return {'Predicted Response Time is': predictions.tolist()}
