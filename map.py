import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import pandas as pd

# Load the dataset
df = pd.read_csv('/Users/weixuanhuang/Desktop/CS 779/final/Bos_food.csv')

# Clean data: Convert columns to numeric and drop rows with NaN values in lat or lon
df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
df = df.dropna(subset=['lat', 'lon'])


# Create a new figure with a specific size
plt.figure(figsize=(12, 12))

llcrnrlat, urcrnrlat = 42.23, 42.42  
llcrnrlon, urcrnrlon = -71.2, -70.95 

# Set up the basemap projection (Mercator projection) focused on Boston
m = Basemap(projection='merc', llcrnrlat=llcrnrlat, urcrnrlat=urcrnrlat,
            llcrnrlon=llcrnrlon, urcrnrlon=urcrnrlon, lat_ts=20, resolution='i')

# Draw coastlines and state boundaries
m.drawcoastlines()
m.drawstates()

# Convert latitude and longitude to x and y coordinates
x, y = m(df['lon'].values, df['lat'].values)

# Use scatter plot to plot the location points
m.scatter(x, y, s=1, color='red', alpha=0.5, zorder=5)

# Optional: add grid lines and title
m.drawparallels(range(int(llcrnrlat), int(urcrnrlat), 1), labels=[1,0,0,0])
m.drawmeridians(range(int(llcrnrlon), int(urcrnrlon), 1), labels=[0,0,0,1])
plt.title('Boston Food Inspection Locations')

# Show the plot
plt.show()
