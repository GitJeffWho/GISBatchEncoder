import pandas as pd
import folium
from folium.plugins import HeatMap
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import numpy as np
from scipy.stats import gaussian_kde
import geopandas as gpd
from shapely.geometry import Point, Polygon


# Creating a .html map with folium and a boundary box

el_path = r''
file_name = ''

# This portion is just for the bounding box, skip if wanted
el_df = pd.read_csv(fr'{el_path}\{file_name}', low_memory=False)

# Subset if needed, on NAME in this case (In the case of multiple bounding boxes stored)
# Check dataframe for other columns as needed again
el_df = el_df[el_df['NAME']=='']

# Calculate center point for map
# i.e. the .html's initial view
center_lat = el_df['POINT_Y'].mean()
center_lon = el_df['POINT_X'].mean()

# Create map centered on average coordinates
m = folium.Map(location=[center_lat, center_lon], zoom_start=14)

shapely_coordinates = [[coord[1], coord[0]] for coord in el_df[['POINT_Y', 'POINT_X']].values.tolist()]
boundary_polygon = Polygon(shapely_coordinates)

# Need separate folium coordinates and shapely coords
# Due to Lat/Long vs Long/Lat input differences
folium_coordinates = el_df[['POINT_Y', 'POINT_X']].values.tolist()

# Feature/layer groups
boundary_group = folium.FeatureGroup(name='Boundary')
granular_group = folium.FeatureGroup(name='Points', show=False)
contours_group = folium.FeatureGroup(name='KDE Contours', show=False)

# Blue Boundary Drawing
folium.PolyLine(
    folium_coordinates,
    weight=2,
    color='blue',
    opacity=0.8,
    name='Boundary'
).add_to(boundary_group)



# Read geocode, this portion is for plotting the actual points, along with KDE and Heatmap creation
geocoded_df = pd.read_csv('.csv')

# print them out for observation
# print(geocoded_df.columns)

gdf = gpd.GeoDataFrame(
    geocoded_df,
    geometry=gpd.GeoSeries.from_wkt(geocoded_df['geometry']),
    crs="EPSG:4326"  # Assuming the coordinates are in WGS84
)

# For individual inbounds/outbounds points
for idx, row in gdf.iterrows():
    # Check if point is inside boundary
    is_inside = boundary_polygon.contains(row['geometry'])

    # Create marker with appropriate color
    # Adjust as necessary
    folium.CircleMarker(
        location=[row['geometry'].y, row['geometry'].x],  # GeoPandas makes it easy to access coordinates
        radius=3,
        color='green' if is_inside else 'red',
        fill=True,
        fill_opacity=0.8,
        # Add address for the popup below, else disable this if you don't want address displayed
        # in the actual .html, sensitive info
        popup=f"Address: {row['Address']}" if 'Address' in gdf.columns else None,
        name='Granular'
    ).add_to(granular_group)

# For Density visualization
# Convert your points to a list of [latitude, longitude] coordinates
locations = [[point.y, point.x] for point in gdf['geometry']]

# Add the heatmap layer
# Add custom gradient if wanted, this is just a basic heatmap layer with the inbuilt settings
HeatMap(
    locations,
    radius=15,
    blur=15,
    min_opacity=0.6,
    name='Heat Map'
    # gradient={0.4: 'blue', 0.65: 'lime', 1: 'red'} Example
    ).add_to(m)


# Also for density visualization, but with KDE instead of heatmap using gaussian layer
# KDE layer function for Folium
def create_kde_layer(gdf, num_cells=200):
    bounds = gdf.total_bounds
    x = np.linspace(bounds[0], bounds[2], num_cells)
    y = np.linspace(bounds[1], bounds[3], num_cells)
    xx, yy = np.meshgrid(x, y)

    positions = np.vstack([xx.ravel(), yy.ravel()])
    points = np.vstack([gdf.geometry.x, gdf.geometry.y])

    kde = gaussian_kde(points)
    density = kde(positions)

    # Reshape density back to grid
    density = density.reshape(num_cells, num_cells)

    return x, y, density


x, y, density = create_kde_layer(gdf)

# Normalize density for color scaling
density_normalized = (density - density.min()) / (density.max() - density.min())

# Create a custom colormap that goes from transparent to red
# Create RGBA array where alpha channel is based on density
rgba_img = np.zeros((density_normalized.shape[0], density_normalized.shape[1], 4))
rgba_img[..., 0] = 1.0  # Red channel
rgba_img[..., 3] = density_normalized  # Alpha channel
rgba_img = (rgba_img * 255).astype(np.uint8)

# Create the overlay with corrected bounds
# Note the [::-1] to flip the image vertically
folium.raster_layers.ImageOverlay(
    rgba_img[::-1],  # Flip array vertically (The array is upside down)
    bounds=[[y.min(), x.min()], [y.max(), x.max()]],
    opacity=0.7,
    name='KDE'
).add_to(contours_group)


# For our contour KDE lines, as KDE by itself is kinda hard to see
fig, ax = plt.subplots()
# Generate contours (adjust levels as needed)
contours = ax.contour(x, y, density, levels=10)
plt.close()  # Close the matplotlib figure since we don't need to display it

# Add contour lines to the map
# matplotlib's collections is deprecated and going to be removed like tomorrow
# So I should really fix that but...
for i, line in enumerate(contours.collections):
    for path in line.get_paths():
        vertices = path.vertices
        # Convert the contour coordinates to lat/lon pairs
        coords = [[lat, lon] for lon, lat in vertices]
        # Add the contour line to the map
        folium.PolyLine(
            coords,
            weight=1,
            color='black',
            opacity=0.5,
            name='KDE Contours'
        ).add_to(contours_group)


boundary_group.add_to(m)
granular_group.add_to(m)
contours_group.add_to(m)


# Add layer control to toggle between heatmap and KDE
folium.LayerControl().add_to(m)


# Save map
# Add output filename here, extension .html
m.save('.html')
