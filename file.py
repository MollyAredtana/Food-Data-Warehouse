import pandas as pd

df = pd.read_csv('/Users/weixuanhuang/Desktop/CS 779/final/food.csv')

# handle data here

# for location we need to make it to be lat, lon using column location
#based on the format
df[['lat', 'lon']] = df['location'].str.extract(r'\((.*), (.*)\)')

df['lat'] = pd.to_numeric(df['lat'])
df['lon'] = pd.to_numeric(df['lon'])

# then we can drop location
df = df.drop(columns=['location'])

# and set new two columns to be lat_prev and lon_prev
df['lat_prev'] = df['lat'].shift(1)
df['lon_prev'] = df['lon'].shift(1)

df.insert(0, 'Unique_Key', range(1, 1 + len(df)))

df['property_id'] = df['property_id'].fillna(0).astype(int)

# some zip code are in wrong format like 02108 -> 2108.0
def format_zip(zip_code):
    if pd.isna(zip_code) or zip_code == '':
        return '00000'  # Default for missing or empty ZIP codes
    elif '.' in zip_code:
        # Remove the decimal and format as a five-digit number
        zip_code = f"{int(float(zip_code)):05d}"
        return zip_code
    else:
        return zip_code # keep it the same

# Apply the function to the 'zip' column
df['zip'] = df['zip'].astype(str) # convert it to string first
df['zip'] = df['zip'].apply(format_zip)

# Convert date strings to datetime objects
df['issdttm'] = pd.to_datetime(df['issdttm'])
df['expdttm'] = pd.to_datetime(df['expdttm'])
# and for the license start-effective-date and end-effective-date
df['start_date'] = df['expdttm']
df['end_date'] = df['expdttm'].shift(-1) - pd.Timedelta(days=1)

df.to_csv('/Users/weixuanhuang/Desktop/CS 779/final/Bos_food.csv', index=False)