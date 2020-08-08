import pandas as pd
import requests

resp = requests.get(f'https://x8qgzno5cd.execute-api.eu-central-1.amazonaws.com/?lat=53.3498&lon=-6.2603&parameter=no2&radius=10000&agg_sensors=1')
df = pd.DataFrame.from_records(resp.json()['results'])
df.plot(x='s5_val', y='openaq_val', kind='scatter', xlim=(df['s5_val'].min() - 5e-6, df['s5_val'].max() + 5e-6))
