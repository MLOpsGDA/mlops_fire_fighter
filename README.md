# mlops_fire_fighter
This is the repository for our group project in the MLOps Training.

## REQUIREMENTS
Start by installing the required packages and dependencies:

``pip install -r requirements
``

## API
 - Launch the API as follows: 
 
 ``uvicorn api.main:app --reload
 ``
 - You can then either access the webservice at the following address:
 ``http://127.0.0.1:8000/docs``
 - You may also choose to write the commands in the terminal. Below are a few examples:

 ``curl -X 'GET' http://127.0.0.1:8000/
 `` for a health check

 ``curl -X 'POST' \ 'http://127.0.0.1:8000/predict' \ -H 'accept: application/json' \ -H 'Content-Type: application/json' \ -d '{ "HourOfCall": 0, "IncidentGroup": "Fire", "StopCodeDescription": "Primary Fire", "PropertyCategory": "Road Vehicle", "PropertyType": "House - single occupancy", "AddressQualifier": "Within same building", "ProperCase": "Ealing", "IncGeo_WardName": "WEST END", "NumStationsWithPumpsAttending": 2.0, "NumPumpsAttending": 3.0, "NumCalls": 4.0, "DateOfCall_Month": 6, "PartOfDay": "Night", "NationalCost": 2000.0 }'
 `` is an example of a possible input
