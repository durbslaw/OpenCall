
import pandas as pd
from datetime import timedelta

# Load Excel file
input_path = 'data/Data Engineer - ipdr.xlsx'
df = pd.read_excel(input_path)

# Normalize columns
df.columns = df.columns.str.lower()
df = df.rename(columns={'appname': 'domain'})
df['starttime'] = pd.to_datetime(df['starttime'])
df['endtime'] = pd.to_datetime(df['endtime'])
df = df.rename(columns={'starttime': 'st', 'endtime': 'et'})

# Compute adjusted ET*
df['et_star'] = df['et'] - timedelta(minutes=10)
df['et_star'] = df[['et_star', 'st']].max(axis=1)

# Sort and group to assign call_id
df = df.sort_values(by=['msisdn', 'domain', 'st'])
df['call_id'] = (df.groupby(['msisdn', 'domain'])['st']
                 .apply(lambda x: (x > x.shift(fill_value=x.min()) + timedelta(minutes=10)).cumsum()))

# Calculate volume in KB
df['total_kb'] = (df['ulvolume'] + df['dlvolume']) / 1024

# Group by call to compute metrics
grouped = df.groupby(['msisdn', 'domain', 'call_id']).agg(
    fdr_count=('call_id', 'count'),
    total_volume_kb=('total_kb', 'sum'),
    start_time=('st', 'min'),
    end_time=('et', 'max')
).reset_index()

# Compute duration and bitrate
grouped['call_duration_sec'] = (grouped['end_time'] - grouped['start_time']).dt.total_seconds()
grouped['kbps'] = grouped['total_volume_kb'] / (grouped['call_duration_sec'] + 1e-6)

# Filter and classify
grouped = grouped[grouped['kbps'] >= 10]
grouped['isAudio'] = grouped['kbps'] <= 200
grouped['isVideo'] = grouped['kbps'] > 200

# Save to output
output_path = 'processed_calls.csv'
grouped.to_csv(output_path, index=False)

print(f"Processed call data saved to {output_path}")
