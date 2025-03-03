# -*- coding: utf-8 -*-
CryptoCompare ETH Data Load into BQ

Connect to a Runtime and begin!

This code ingests data from crypto compare API and loads it into BigQuery Table
"""

# install pandas-gbq package to enable interaction of dataframes with bigquery
pip install pandas-gbq

# import all relevant packages
import pandas as pd
from pandas.io import gbq
import requests
import json

# call cryptocompare historical stats API

# api_key should be yours
def get_data():
    crypto_api_url = "https://min-api.cryptocompare.com/data/v2/histoday"
    payload = {'fsym': 'ETH', 'tsym': 'USD','limit': '2000', 'api_key': '22ec95cfb7ee312a1375a9eb1b331a309cb5e472b8d28190d9545b321ba110aa3'}
    request_data = requests.get(crypto_api_url,params=payload)
    return request_data.json()['Data']['Data']

CryptoAPIDataResponse = get_data()

CryptoAPIDataResponse

#convert to JSON
CryptoAPIDataResponseJSON = json.dumps(CryptoAPIDataResponse)
print(CryptoAPIDataResponseJSON)

#convert to pandas dataframe to prepare for insertion into BigQuery
datalistDF = pd.read_json(CryptoAPIDataResponseJSON)
datalistDF.head()

# load data to bigquery using service account credentials

from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_info(
{
  "type": "service_account",
  "project_id": "your_bigquery_folder",
  "private_key_id": "your_private_key_id",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDLclE8etm0CTQY\nWYAiQbPtCkdggr2t9CvH/rdNPE/h5QyrIeSk+4Exj8rsT0mw1unhgjztGOXgIb9N\nM83zxeDt1jx3atucnbYGf/JcO9eWVvPL5lRLqHlWUhvUHxZ9F88/milBUkhgBeId\nJYfMe7ZuXtzGDz2qCdNw3/3N6nk4YMUTAnprxbhXXYT4QuZnp4xwk5Ajh7tEfZbu\nrsuO1wRGXCAnXxzIhAva6q4n1sygtTbs8WNRVxnFQaaJbfqry5ePUnHXlN0DArWe\nTB8fVu05MhPqF6ItyTgofl3bUOWN6MYtUsy/QvLnKVTKUU+9ndvz2egGzeFEz5nr\n/TDfW6VrAgMBAAECggEAIwEMUSeiBYP/3qCnlz1Ow94c4dIc6K02SzbcOwHBjNBo\nGZm/SVOsquZet8H8n9yaaR6LdQ/vnVyxGYtsSCEnzkQqKzHLj2rjQsMI5C23rjpP\nwCllH49hMST3fMvMhsFt+zd/GaoaocrhWLp4Xwp6HQtdWQaOSY9fPfCMLr3FFZni\nJ+/KFZfeJ71fXbN9Hrsqh5LWvPr9TNmU3yrXzJUZ7Oug6sHYVwRCdxxoEvg7+Ilq\nrm4QQUcO6FoLVHkhSMh2eVP9lyOg8E3KH13Ri8RBLUyilEyibQuHt5DDuQnEz/vL\nWR1BJp5zwfEtT4zBVadZ9ghjqZ1l4vAUrA1aU6p8AQKBgQDxGWnwftW0RW+L3Mxx\nGWzlk2wTfxja3c2ZlH3T+eOg/a4MLO9cUxPTsH6VCb6+yVpOhmr4mFM2edI66pBZ\n0ZA4taxUb7v5hd9ixXWOKltC30S9Gp96vZTPZhBtKzB9doqXkFkrzxVLZ14wPsdJ\nqxvTfVuMcAeQayCjzowN+L2v6wKBgQDYBSz4mCpSuPG3eJJGCGNOn3YrMBAyxYVc\nDXYZSUiaivAOkPDRDs6KLv0NmTp++dURc36Lyxo5N29WOPa+UrNCeeBw4cfQa3Vc\n5qSZRpUd64yJMKgKhZePGlMqrRiOAtSd1nIV0QnpPQZ1zj1EZ9sg82XkCM+XCmeu\nu8XPa9IAgQKBgQDnjm/4Aej/xXx3iK11pmS3LVqscvINKt/PNBWRZDwiH+JsYS/W\nrReqxaUdQQnItRwdtO3CKpIpDOFDoorXKPVp8do8EkAoS1Iby+e2jamu1T5wnMQ0\nnyXv92epYcSlixdar9FkyPP7wqBsl67RaxxDh+9IN0SciLcFfFd5B8Au1wKBgAOt\nsuJY1P+rnpIdP4BpzCcemAiMPPpgWrECmaw8jzvyebwFw9QLiIDZ4/1DSre20WYG\nrXT79GpdA0xYk/sRtXPAV44Uii6GZe07EDp79TYZOL9tUK89LyOEsz3azLw+WBiH\n+lFcyepq251N8cUkb5rUCjwj2kUcLD/ResDZeS4BAoGBAI+N1uPoZX8c8RJh4rc0\nJYS1tZo/VTIXWzSexzDNiiUvQXpBJZ+vkCikcdI4mXimfttURc6eDjV4X8rYAxJv\nJMizo+whsgyTRiWE5pCaSiu3IKz7+gc/34ttUiyBfNjhDKN6gi6YPfUlLU4Tpbcq\nvBKK3iMUfvHLzlYTaONUxrA6\n-----END PRIVATE KEY-----\n",
  "client_email": "osuproject@my-your_bigquery_folder.iam.gserviceaccount.com",
  "client_id": "your_id",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/your_bigquery_project.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}
)
datalistDF.to_gbq(destination_table='Your_BigQuery_table_name',project_id='your_project_id', if_exists='fail', credentials=credentials)
