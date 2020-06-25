import requests
import pandas as pd
import os

resp = requests.get(f'{os.environ.get("INVOKE_URL")}/?lat=53.3498&lon=-6.2603&parameter=no2')
df = pd.DataFrame.from_records(resp.json()['results'])
df.plot(x='s5_val', y='openaq_val', kind='scatter', xlim=(df['s5_val'].min() - 5e-6, df['s5_val'].max() + 5e-6))
