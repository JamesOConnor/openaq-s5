import os
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial

import pandas as pd
import requests

from src import RequestValidator, unpack_query_parameters, retrieve_measurement, get_s5_keys, aggregate_by_day


def lambda_handler(event, context):
    """
    Process incoming request
    """
    lat, lon, limit, radius, parameter = unpack_query_parameters(event)
    errors = RequestValidator().validate(
        {'lat': lat, 'lon': lon, 'limit': limit, 'radius': radius, 'parameter': parameter})
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

    openaq_daily_mean = aggregate_by_day(results)
    unit = results[0]['unit']

    s5_df = get_s5_keys(parameter)
    uris_merged_to_openaq = pd.merge(openaq_daily_mean, s5_df, on='date')

    _worker = partial(retrieve_measurement, lat=lat, lon=lon)
    with ThreadPoolExecutor(max_workers=len(uris_merged_to_openaq)) as executor:
        data = list(executor.map(_worker, uris_merged_to_openaq['uri'].tolist()))
    uris_merged_to_openaq['s5_val'] = data
    filtered_nodata = uris_merged_to_openaq[uris_merged_to_openaq['s5_val'] != -9999]
    filtered_nodata['date'] = filtered_nodata['date'].dt.strftime('%Y-%m-%d')

    out_json = {}
    out_json['results'] = filtered_nodata[['date', 's5_val', 'openaq_val']].to_dict(orient='records')
    out_json['meta'] = {'found': len(filtered_nodata), 'parameter': parameter, 's5_unit': 'mol/m\u00b2',
                        'openaq_unit': unit}
    return out_json
