import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from pathlib import Path

# --- Define new file paths ---
listings_path = Path("01_Data/Cleaned/listings.parquet")
boroughs_path = Path("statistical-gis-boundaries-london/ESRI/London_Borough_Excluding_MHW.shp")
msoas_path    = Path("statistical-gis-boundaries-london/ESRI/MSOA_2011_London_gen_MHW.shp")

# --- Load cleaned listings (Parquet) ---
df_l = pd.read_parquet(listings_path)

# --- Load shapefiles ---
boroughs = gpd.read_file(boroughs_path)
msoas = gpd.read_file(msoas_path)

# Convert shapefiles to EPSG:27700 (British National Grid)
boroughs = boroughs.to_crs(epsg=27700)
msoas = msoas.to_crs(epsg=27700)

# Listings → GeoDataFrame in BNG
gdf_l = gpd.GeoDataFrame(
    df_l,
    geometry=gpd.points_from_xy(df_l.longitude, df_l.latitude),
    crs="EPSG:4326"
).to_crs(epsg=27700)

# --- Identify professional hosts ---
prof_hosts = df_l[
    (df_l["host_listings_count"] >= 2) &
    (df_l["availability_365"] >= 180)
]["host_id"].unique()

df_prof_hosts = df_l[df_l["host_id"].isin(prof_hosts)]

# Convert professional hosts to GeoDataFrame
gdf_prof_hosts = gpd.GeoDataFrame(
    df_prof_hosts,
    geometry=gpd.points_from_xy(df_prof_hosts.longitude, df_prof_hosts.latitude),
    crs="EPSG:4326"
).to_crs(epsg=27700)
