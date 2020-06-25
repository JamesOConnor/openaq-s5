import os
import re

import boto3
import numpy as np
import pandas as pd
import requests
from marshmallow import Schema, fields
from marshmallow.validate import Range, OneOf


class RequestValidator(Schema):
    """
    Validate incoming requests
    """
    lat = fields.Float(required=True, validate=Range(min=-90, max=90))
    lon = fields.Float(required=True, validate=Range(min=-180, max=180))
    parameter = fields.Str(required=False, validate=OneOf(('co', 'no2', 'o3', 'so2')))
    limit = fields.Int(required=False, validate=Range(min=1, max=10000))
    radius = fields.Int(required=False, validate=Range(min=1, max=10000))


def unpack_query_parameters(event):
    """
    Get the query parameters from the lambda trigger
    :param event: lambda event body
    :return:
    """
    lat = float(event.get('queryStringParameters').get('lat'))
    lon = float(event.get('queryStringParameters').get('lon'))
    parameter = event.get('queryStringParameters').get('parameter') or 'no2'
    radius = event.get('queryStringParameters').get('radius') or 2500
    limit = event.get('queryStringParameters').get('limit') or 1000
    return lat, lon, limit, radius, parameter


def retrieve_measurement(uri, lat, lon):
    """
    Call a sampling lambda to do retrieve the value at a given point, for a given image
    :param uri: s3 uri
    :param lat: latitude as a float
    :param lon: longitude as a float
    :return: the value as a float
    """
    body = {'uri': uri, 'lat': lat, 'lon': lon}
    r = requests.post(os.environ.get('SAMPLE_URL'), json=body)
    return float(r.json()['val'])


def get_s5_keys(parameter):
    """
    For a given parameter, generate keys and dates for that parameter from the L3 data on the meeo-s5p bucket
    :param parameter: parameter to query (eg: so2)
    :return: pandas dataframe with uri and date fields
    """
    s3_cnxn = boto3.resource('s3')
    s3_bucket = s3_cnxn.Bucket('meeo-s5p')
    keys = list(s3_bucket.objects.filter(Prefix=f'COGT/NRTI/L3__{parameter.upper()}'))
    uris = ['s3://meeo-s5p/' + i.key for i in keys if 'column_4326.tif' in i.key]
    dates_for_uris = [re.search(r'\d{4}\d{2}\d{2}', i).group() for i in uris]
    s5_df = pd.DataFrame(np.array([uris, dates_for_uris]).T, columns=['uri', 'date'])
    s5_df['date'] = pd.to_datetime(s5_df['date'])
    return s5_df


def aggregate_by_day(results):
    """
    Average the results from openaq by day
    :param results: results from the openaq query
    :return: pandas dataframe of date and mean value
    """
    date_vals = [[i['date']['utc'].split('T')[0], float(i['value'])] for i in results]
    openaq_df = pd.DataFrame(date_vals, columns=['date', 'openaq_val'])
    openaq_daily_mean = openaq_df.groupby('date').mean().reset_index()
    openaq_daily_mean['date'] = pd.to_datetime(openaq_daily_mean['date'])
    return openaq_daily_mean