import rasterio as rio
import json

def retrieve_measurement(uri, lat, lon):
    im = rio.open(uri)
    return list(im.sample([(lon, lat)]))[0][0]

def lambda_handler(event, context):
    if 'body' in event.keys():
        event = json.loads(event['body'])
    lat = float(event['lat'])
    lon = float(event['lon'])
    uri = event['uri']
    d = retrieve_measurement(uri, lat, lon)
    return json.dumps({'val': str(d)})