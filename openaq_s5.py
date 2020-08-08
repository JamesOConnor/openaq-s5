import os
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial

import requests

from src import RequestValidator, unpack_query_parameters, retrieve_measurement, get_s5_keys, aggregate_by_day, \
    aggregate_by_day_and_sensor


def lambda_handler(event, context):
    """
    Process incoming request
    """
    lat, lon, limit, radius, parameter, aggregate_sensors = unpack_query_parameters(event)
    errors = RequestValidator().validate(
        {'lat': lat, 'lon': lon, 'limit': limit, 'radius': radius, 'parameter': parameter,
         'agg_sensors': aggregate_sensors})
    if errors:
        return errors
    openaq = requests.get(
        f'{os.environ.get("OPENAQ_URL")}?coordinates={lat},{lon}&parameter={parameter}&limit={limit}&radius={radius}')
    if openaq.ok:
        results = openaq.json()['results']
    else:
        return {"message": "openaq proxy error"}
    if len(results) == 0:
        return {"message": "No data for this query"}

    if aggregate_sensors:
        openaq_merged = aggregate_by_day(results)
    else:
        openaq_merged = aggregate_by_day_and_sensor(results)

    unit = results[0]['unit']
    s5_df = get_s5_keys(parameter)
    s5_df = s5_df[s5_df['date'].isin(openaq_merged['date'])]

    # Retrieve the data from S5
    _worker = partial(retrieve_measurement, lat=lat, lon=lon)
    with ThreadPoolExecutor(max_workers=len(s5_df)) as executor:
        data = list(executor.map(_worker, s5_df['uri'].tolist()))

    # Add the data to the dataframe
    s5_df['s5_val'] = data
    s5_df = s5_df[s5_df['s5_val'] != -9999]

    # Merge the S5 data with openaq data
    openaq_merged = openaq_merged[openaq_merged['date'].isin(s5_df['date'])]
    openaq_merged['s5_val'] = openaq_merged['date'].apply(lambda x: s5_df[s5_df['date'] == x]['s5_val'].iloc[0])

    if aggregate_sensors:
        openaq_merged['date'] = openaq_merged['date'].dt.strftime('%Y-%m-%d')
        out_json = {}
        out_json['results'] = openaq_merged[['date', 's5_val', 'openaq_val']].to_dict(orient='records')
        out_json['meta'] = {'found': len(openaq_merged), 'parameter': parameter, 's5_unit': 'mol/m\u00b2',
                            'openaq_unit': unit}
        return out_json
    else:
        openaq_merged['date'] = openaq_merged['date'].dt.strftime('%Y-%m-%d')
        grouped = openaq_merged.groupby('location_name')
        out_json = {}
        out_json['results'] = {
            group: grouped.get_group(group)[['openaq_val', 's5_val', 'date']].to_dict(orient='records') for group in
            grouped.groups}
        out_json['meta'] = {'found': len(openaq_merged), 'parameter': parameter, 's5_unit': 'mol/m\u00b2',
                            'openaq_unit': unit}
        return out_json

